"""Sinh toàn bộ số liệu và biểu đồ cho báo cáo: report/SO_LIEU.md + report/figures/*.png.

Không đọc số từ output/ hay gold/ (có thể là kết quả của một lần chạy khác tham số): chạy lại ĐÚNG logic
Silver (silver.transform) và Gold (các câu SQL của gold.py) trên Bronze active, cho từng phiên bản ref,
rồi tính số liệu từ đó. Nhờ vậy mọi con số trong báo cáo khớp với code, và đối chiếu được với Silver
đang nằm trên đĩa (kiểm ở bước đầu).

Hai phạm vi số liệu — ghi rõ ở mỗi bảng:
  (A) HỆ THỐNG    tất cả lô, ref mới nhất — trạng thái pipeline hiện tại (số kỹ thuật, slide Section 3–4)
  (B) PHÁT HIỆN   chỉ các lô có thông tin xe THẬT (manifest không có is_vehicle_attributes_assumed), ref
                  phiên bản đầu (đã duyệt thật) — dùng cho kết luận về giá xe, không để dữ liệu giả định
                  làm lệch kết luận.

Chạy (sau python -m src.run_pipeline):  python -m src.report
"""
import json
import logging
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402
import pandas as pd  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src import gold, reference, silver  # noqa: E402
from src.config import (BRONZE_DIR, LANDING_DIR, ML_TRAIN_END_MONTH, PARTITION_COL,  # noqa: E402
                        REF_MODEL_MAP_NAME, REF_VEH_TYPE_NAME, ROOT, RUN_INFO_FILE, RUN_LOG,
                        SILVER_DIR)

log = logging.getLogger("report")

REPORT_DIR = ROOT / "report"
FIG_DIR = REPORT_DIR / "figures"

# ---- Kiểu biểu đồ: palette đã kiểm bằng validator (dataviz), nền sáng cho slide ------------------
SURFACE, INK, INK_2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
C1, C2, OTHER = "#2a78d6", "#eb6834", "#c3c2b7"          # slot 1 xanh · slot 2 cam · "khác" xám
plt.rcParams.update({
    "font.family": ["Segoe UI", "DejaVu Sans"], "font.size": 11,
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": AXIS, "axes.labelcolor": INK_2, "axes.titlecolor": INK,
    "axes.titlesize": 14, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "axes.grid.axis": "y", "grid.color": GRID, "grid.linewidth": 0.8,
    "xtick.color": MUTED, "ytick.color": MUTED, "xtick.labelcolor": INK_2, "ytick.labelcolor": INK_2,
    "legend.frameon": False, "legend.labelcolor": INK_2,
})


# ---- Định dạng số kiểu Việt Nam ----------------------------------------------------------------
def n(x) -> str:
    return f"{int(x):,}".replace(",", ".")


def p(x, d: int = 2) -> str:
    return f"{x:.{d}f}".replace(".", ",")


def md_table(df: pd.DataFrame) -> str:
    head = "| " + " | ".join(map(str, df.columns)) + " |"
    sep = "|" + "|".join("---" for _ in df.columns) + "|"
    rows = ["| " + " | ".join(map(str, r)) + " |" for r in df.itertuples(index=False)]
    return "\n".join([head, sep, *rows])


# ---- 1. Dựng lại Silver/Gold trong bộ nhớ cho một phiên bản ref --------------------------------
def assumed_months() -> list[str]:
    out = []
    for m in sorted(LANDING_DIR.glob(f"{PARTITION_COL}=*/_manifest.json")):
        if json.loads(m.read_text(encoding="utf-8")).get("is_vehicle_attributes_assumed"):
            out.append(m.parent.name.split("=", 1)[1])
    return out


def build(spark: SparkSession, bronze, ref_as_of: str | None, exclude_months: list[str]) -> dict:
    """Silver + 4 câu SQL của Gold trên một phạm vi tháng. Không ghi đĩa."""
    refs = {k: reference.inspect(reference.resolve(k, ref_as_of)) for k in (REF_MODEL_MAP_NAME, REF_VEH_TYPE_NAME)}
    dim_model = silver.build_dim_model(silver.read_ref_csv(spark, refs[REF_MODEL_MAP_NAME].path)).cache()
    dim_type = silver.build_dim_vehicle_type(silver.read_ref_csv(spark, refs[REF_VEH_TYPE_NAME].path)).cache()
    scoped = bronze.filter(~F.col(PARTITION_COL).isin(exclude_months)) if exclude_months else bronze
    fact, quarantine = silver.transform(scoped, dim_model, dim_type)
    fact, quarantine = fact.cache(), quarantine.cache()
    dim_date = silver.build_dim_date(spark, fact)
    for name, df in {"fact_valuation": fact, "quarantine": quarantine, "dim_model": dim_model,
                     "dim_vehicle_type": dim_type, "dim_date": dim_date}.items():
        df.createOrReplaceTempView(name)
    rule_counts = {r.rule_id: r["count"] for r in quarantine.groupBy("rule_id").count().collect()}
    return {
        "refs": refs,
        "bronze": scoped.count(), "fact": fact.count(), "quarantine": quarantine.count(),
        "dims": {"dim_model": dim_model.count(), "dim_vehicle_type": dim_type.count(),
                 "dim_date": dim_date.count()},
        "hang": dim_model.select("hang_xe").distinct().count(),
        "hieu": dim_model.select("hang_xe", "hieu_xe").distinct().count(),
        "rules": {r: rule_counts.get(r, 0) for r in silver.RULES},
        "dq": spark.sql(gold.mart_data_quality_sql()).toPandas(),
        "mvp": spark.sql(gold.MART_VEHICLE_PRICE).toPandas(),
        "mmt_rows": spark.sql(gold.MART_MONTHLY_TREND).count(),
        "ml": spark.sql(gold.mart_ml_training_sql(ML_TRAIN_END_MONTH)).toPandas(),
    }


def check_against_disk(a: dict) -> None:
    """Phạm vi (A) phải khớp Silver đang nằm trên đĩa — nếu không, báo cáo đang mô tả một trạng thái khác."""
    info = json.loads((SILVER_DIR / RUN_INFO_FILE).read_text(encoding="utf-8"))
    got = (a["bronze"], a["fact"], a["quarantine"], a["refs"][REF_MODEL_MAP_NAME].version)
    want = (info["counts"]["bronze"], info["counts"]["fact"], info["counts"]["quarantine"],
            info["refs"][REF_MODEL_MAP_NAME]["version"])
    if got != want:
        raise RuntimeError(f"Số dựng lại {got} ≠ Silver trên đĩa {want} — chạy python -m src.run_pipeline trước")


# ---- 2. Số liệu thô từ landing -----------------------------------------------------------------
def landing_stats(months_excluded: list[str]) -> dict:
    frames = []
    for f in sorted(LANDING_DIR.glob(f"{PARTITION_COL}=*/*.csv")):
        if f.parent.name.split("=", 1)[1] not in months_excluded:
            frames.append(pd.read_csv(f, dtype=str, encoding="utf-8-sig", keep_default_na=False))
    raw = pd.concat(frames, ignore_index=True)
    price = pd.to_numeric(raw["gia_tri_dinh_gia"], errors="coerce")
    per_lot = raw.groupby(PARTITION_COL).size()
    filled = lambda c: (raw[c].str.strip() != "").mean() * 100
    codes = set(raw["ma_dong_xe"].str.strip()) - {""}
    out = {"rows": len(raw), "lots": len(per_lot), "lot_min": per_lot.min(), "lot_max": per_lot.max(),
           "first": per_lot.index.min(), "last": per_lot.index.max(),
           "price_zero": int((price == 0).sum()), "hang_xe": filled("hang_xe"),
           "ma_kieu_dang": filled("ma_kieu_dang"), "n_codes": len(codes), "code_cover": {}}
    for v in reference.list_versions(REF_MODEL_MAP_NAME):
        ref = pd.read_csv(reference.resolve(REF_MODEL_MAP_NAME, v), dtype=str, encoding="utf-8-sig")
        known = set(ref["ma_dong_xe"])
        out["code_cover"][v] = {"codes_pct": len(codes & known) / len(codes) * 100,
                                "rows_unknown_pct": (~raw["ma_dong_xe"].str.strip().isin(known)).mean() * 100}
    return out


# ---- 3. Phát hiện về tỷ lệ còn lại ρ --------------------------------------------------------------
def rho_stats(mvp: pd.DataFrame) -> dict:
    rho = mvp[mvp["ty_le_con_lai"].notna()]
    by_year = rho.groupby("nam_san_xuat")["ty_le_con_lai"].agg(["count", "median"]).reset_index()
    return {"groups": len(mvp), "both": len(rho), "median": rho["ty_le_con_lai"].median(),
            "min": rho["ty_le_con_lai"].min(), "max": rho["ty_le_con_lai"].max(),
            "gt1": int((rho["ty_le_con_lai"] > 1).sum()), "by_year": by_year, "rho": rho}


# ---- 4. Biểu đồ ----------------------------------------------------------------------------------
def save(fig, name: str) -> Path:
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    path = FIG_DIR / name
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    return path


def shade_assumed(ax, months: list[str], assumed: list[str], label_y: float) -> None:
    """Nền xám nhạt cho các lô có thông tin xe giả định — người xem biết đọc các cột đó khác đi."""
    idx = [months.index(m) for m in assumed if m in months]
    if idx:
        ax.axvspan(min(idx) - 0.5, max(idx) + 0.5, color="#f0efec", zorder=0, lw=0)
        ax.text((min(idx) + max(idx)) / 2, label_y, "lô giả định\n(BSS)", ha="center", va="top",
                fontsize=9, color=MUTED)


def fig_quarantine_by_month(dq: pd.DataFrame, assumed: list[str]) -> Path:
    d = dq.sort_values("as_of_month").reset_index(drop=True)
    months = d["as_of_month"].tolist()
    pct = lambda c: 100 * c / d["so_dong_bronze"]
    model = pct(d["model_code_unknown"])
    price = pct(d["price_invalid"])
    other = pct(d[["corrupt_record", "dup_key", "missing_field", "date_invalid", "out_of_scope",
                   "value_invalid"]].sum(axis=1))
    fig, ax = plt.subplots(figsize=(11, 5.2))
    x = range(len(months))
    kw = dict(width=0.72, edgecolor=SURFACE, linewidth=1.5, zorder=2)
    ax.bar(x, model, color=C1, label="MODEL_CODE_UNKNOWN", **kw)
    ax.bar(x, price, bottom=model, color=C2, label="PRICE_INVALID", **kw)
    ax.bar(x, other, bottom=model + price, color=OTHER, label="Luật khác (5 luật)", **kw)
    total = model + price + other
    ax.set_xticks(list(x), [m[2:] for m in months], rotation=0, fontsize=9)
    ax.set_ylabel("% dòng Bronze active bị tách")
    ax.set_ylim(0, total.max() * 1.22)
    hi = int(total.idxmax())
    ax.annotate(f"{p(total[hi])}%", (hi, total[hi]), xytext=(0, 4), textcoords="offset points",
                ha="center", fontsize=9, color=INK)
    shade_assumed(ax, months, assumed, total.max() * 1.2)
    ax.set_title("Mã xe chưa giải mã được là nguyên nhân chính khiến dòng bị tách vào quarantine")
    ax.legend(loc="upper left", ncol=3, bbox_to_anchor=(0, 1.0))
    return save(fig, "hinh_05_quarantine_theo_thang.png")


def fig_rho_by_year(rs: dict) -> Path:
    rho = rs["rho"].copy()
    years = sorted(rho["nam_san_xuat"].unique())
    fig, ax = plt.subplots(figsize=(9, 5.2))
    ax.grid(axis="y")
    for i, y in enumerate(years):
        g = rho[rho["nam_san_xuat"] == y].sort_values("ty_le_con_lai").reset_index(drop=True)
        jitter = [((k * 0.6180339) % 1 - 0.5) * 0.36 for k in range(len(g))]     # tất định, không random
        colors = [C2 if v > 1 else C1 for v in g["ty_le_con_lai"]]
        ax.scatter([i + j for j in jitter], g["ty_le_con_lai"], s=34, c=colors, edgecolors=SURFACE,
                   linewidths=1.2, zorder=3)
        med = g["ty_le_con_lai"].median()
        ax.plot([i - 0.28, i + 0.28], [med, med], color=INK, lw=2, zorder=4)
        ax.text(i + 0.31, med, f"trung vị {p(med, 3)}\n{len(g)} nhóm", va="center", fontsize=9, color=INK_2)
    ax.axhline(1.0, color=MUTED, lw=1, zorder=1)
    ax.text(-0.75, 1.003, "ρ = 1: giá cũ bằng giá mới", ha="left", va="bottom", fontsize=9, color=MUTED)
    ax.set_xticks(range(len(years)), [f"Đời {y}" for y in years])
    ax.set_xlim(-0.8, len(years) - 0.2)
    ax.set_ylabel("ρ = trung vị giá cũ / trung vị giá mới")
    ax.scatter([], [], s=34, c=C1, label="ρ ≤ 1")
    ax.scatter([], [], s=34, c=C2, label=f"ρ > 1 ({rs['gt1']} nhóm)")
    ax.legend(loc="lower right")
    ax.set_title(f"Xe cũ cùng đời được định giá thấp hơn xe mới khoảng {p((1 - rs['median']) * 100, 1)}%; "
                 f"{rs['gt1']} nhóm ngược lại")
    return save(fig, "hinh_06_rho_theo_doi_xe.png")


def fig_ref_versions(per_ref: dict, assumed: list[str]) -> Path:
    fig, ax = plt.subplots(figsize=(11, 5.2))
    months = None
    for (ver, res), color in zip(per_ref.items(), (C1, C2)):
        d = res["dq"].sort_values("as_of_month").reset_index(drop=True)
        months = d["as_of_month"].tolist()
        y = d["ty_le_quarantine_pct"]
        ax.plot(range(len(d)), y, color=color, lw=2, marker="o", ms=5, mec=SURFACE, mew=1.2,
                label=f"ref {ver} — {p(100 * res['quarantine'] / res['bronze'])}% toàn kỳ", zorder=3)
        ax.annotate(f"ref {ver}", (len(d) - 1, y.iloc[-1]), xytext=(8, 0), textcoords="offset points",
                    va="center", fontsize=9, color=INK_2)
    ax.set_xticks(range(len(months)), [m[2:] for m in months], fontsize=9)
    ax.set_ylim(0, max(r["dq"]["ty_le_quarantine_pct"].max() for r in per_ref.values()) * 1.25)
    ax.set_ylabel("% dòng bị tách vào quarantine")
    shade_assumed(ax, months, assumed, ax.get_ylim()[1] * 0.98)
    vers = list(per_ref)
    moved = per_ref[vers[0]]["quarantine"] - per_ref[vers[-1]]["quarantine"]
    ax.set_title(f"Bàn giao ref {vers[-1]} kéo {n(moved)} dòng về fact ở mọi tháng — không sửa code, không gửi lại lô")
    ax.legend(loc="lower left")
    return save(fig, "hinh_07_phien_ban_ref.png")


def fig_contracts_by_month(ml: pd.DataFrame) -> Path:
    c = ml.groupby(["thang_duyet", "loai_dinh_gia"]).size().unstack(fill_value=0).sort_index()
    months = c.index.tolist()
    fig, ax = plt.subplots(figsize=(11, 5.2))
    x = range(len(months))
    kw = dict(width=0.72, edgecolor=SURFACE, linewidth=1.5, zorder=2)
    ax.bar(x, c["used"], color=C1, label="Xe đã qua sử dụng (used)", **kw)
    ax.bar(x, c["new"], bottom=c["used"], color=C2, label="Xe mới (new)", **kw)
    tot = c.sum(axis=1)
    hi = int(tot.values.argmax())
    ax.annotate(n(tot.iloc[hi]), (hi, tot.iloc[hi]), xytext=(0, 4), textcoords="offset points",
                ha="center", fontsize=9, color=INK)
    ax.set_xticks(list(x), [m[2:] for m in months], fontsize=9)
    ax.set_ylabel("Số hợp đồng trong fact")
    ax.set_ylim(0, tot.max() * 1.15)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: n(v)))
    share_new = c["new"].sum() / tot.sum() * 100
    ax.set_title(f"Số hợp đồng cao nhất vào tháng {months[hi]}; xe mới chiếm {p(share_new, 1)}% toàn kỳ")
    ax.legend(loc="upper left")
    return save(fig, "hinh_08_hop_dong_theo_thang.png")


def fig_train_test(ml: pd.DataFrame, assumed: list[str]) -> Path:
    c = ml.groupby("thang_duyet").agg(n=("split", "size"), split=("split", "first")).sort_index()
    months = c.index.tolist()
    fig, ax = plt.subplots(figsize=(11, 5.2))
    colors = [C1 if s == "train" else C2 for s in c["split"]]
    ax.bar(range(len(months)), c["n"], color=colors, width=0.72, edgecolor=SURFACE, linewidth=1.5, zorder=2)
    cut = months.index(ML_TRAIN_END_MONTH) + 0.5
    ax.axvline(cut, color=INK, lw=1.2, zorder=3)
    ax.text(cut + 0.1, c["n"].max() * 1.12, f"mốc chia m₀ = {ML_TRAIN_END_MONTH}", fontsize=9, color=INK, va="top")
    ax.set_xticks(range(len(months)), [m[2:] for m in months], fontsize=9)
    ax.set_ylim(0, c["n"].max() * 1.18)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: n(v)))
    ax.set_ylabel("Số dòng mart_ml_training")
    shade_assumed(ax, months, assumed, c["n"].max() * 1.05)
    tr, te = (c["split"] == "train"), (c["split"] == "test")
    ax.legend(handles=[Patch(color=C1, label=f"train — {n(c.loc[tr, 'n'].sum())} dòng"),
                       Patch(color=C2, label=f"test — {n(c.loc[te, 'n'].sum())} dòng")], loc="upper left")
    ax.set_title("Chia train/test theo tháng duyệt: model luôn được đánh giá trên các tháng chưa thấy")
    return save(fig, "hinh_09_train_test.png")


# ---- 5. SO_LIEU.md --------------------------------------------------------------------------------
def write_markdown(A: dict, B: dict, per_ref: dict, raw_b: dict, rs: dict, assumed: list[str],
                   figs: dict, run_log: pd.DataFrame) -> Path:
    ver_a = A["refs"][REF_MODEL_MAP_NAME].version
    ver_b = B["refs"][REF_MODEL_MAP_NAME].version
    months_a = sorted(A["dq"]["as_of_month"])
    months_b = sorted(B["dq"]["as_of_month"])
    q = lambda r: 100 * r["quarantine"] / r["bronze"]
    dqa = A["dq"]
    ml_a = A["ml"]
    tr, te = ml_a[ml_a.split == "train"], ml_a[ml_a.split == "test"]
    rule_rows = pd.DataFrame([{"Luật": f"`{r}`", "(A) dòng": n(A["rules"][r]),
                               "(A) % quarantine": p(100 * A["rules"][r] / A["quarantine"], 1),
                               "(B) dòng": n(B["rules"][r])} for r in silver.RULES])
    by_year = pd.DataFrame([{"Đời xe": int(r.nam_san_xuat), "Số nhóm đủ hai phía": int(r["count"]),
                             "Trung vị ρ": p(r["median"], 3)} for _, r in rs["by_year"].iterrows()])
    gt1 = rs["rho"][rs["rho"]["ty_le_con_lai"] > 1].sort_values("so_hop_dong", ascending=False)
    gt1_tbl = pd.DataFrame({"Hãng": gt1.hang_xe, "Dòng xe": gt1.hieu_xe, "Đời": gt1.nam_san_xuat,
                            "Chỗ": gt1.so_cho_ngoi, "HĐ mới": gt1.so_hd_xe_moi, "HĐ cũ": gt1.so_hd_xe_cu,
                            "ρ": [p(v, 3) for v in gt1.ty_le_con_lai]})
    dq_tbl = pd.DataFrame({"Tháng": dqa.as_of_month, "Bronze": dqa.so_dong_bronze.map(n),
                           "Fact": dqa.so_dong_fact.map(n), "Quarantine": dqa.so_dong_quarantine.map(n),
                           "% q": dqa.ty_le_quarantine_pct.map(p),
                           "MODEL_CODE_UNKNOWN": dqa.model_code_unknown.map(n),
                           "PRICE_INVALID": dqa.price_invalid.map(n)}).sort_values("Tháng")
    vers = list(per_ref)
    ref_tbl = pd.DataFrame([{
        "Ref": v, "Mã phiên bản": n(r["refs"][REF_MODEL_MAP_NAME].row_count), "Dòng xe": r["hieu"],
        "Hãng": r["hang"], "Fact": n(r["fact"]), "Quarantine": n(r["quarantine"]), "% q": p(q(r)),
        "MODEL_CODE_UNKNOWN": n(r["rules"]["MODEL_CODE_UNKNOWN"])} for v, r in per_ref.items()])
    ml_months = ml_a.groupby("thang_duyet").size()
    unseen = set(te.hieu_xe) - set(tr.hieu_xe)
    cols_log = ["run_id", "ref_as_of", "ref_model_map_version", "train_end_month", "fact", "quarantine",
                "ty_le_quarantine_pct", "ml_train", "ml_test"]
    log_tbl = run_log[cols_log] if not run_log.empty else pd.DataFrame(columns=cols_log)

    cover = raw_b["code_cover"]
    md = f"""# SỐ LIỆU BÁO CÁO — sinh tự động

> Sinh bởi `python -m src.report` từ Bronze active + logic Silver/Gold của code hiện tại. **Không sửa tay**
> file này — sửa code/dữ liệu rồi chạy lại. Biểu đồ ở `report/figures/` (Hình 1–4 là sơ đồ, tự vẽ).

## Phạm vi số liệu

| Ký hiệu | Phạm vi | Lô | Ref `ref_model_map` | Dùng cho |
|---|---|---|---|---|
| **(A) Hệ thống** | Trạng thái pipeline hiện tại | {len(months_a)} lô ({months_a[0]} → {months_a[-1]}) | {ver_a} | Số kỹ thuật: Section 3–4, Slide 14–23 |
| **(B) Phát hiện** | Chỉ dữ liệu thật đã kiểm chứng | {len(months_b)} lô ({months_b[0]} → {months_b[-1]}) | {ver_b} | Kết luận về giá xe: Slide 4, 15, 24 |

Lô có thông tin xe **giả định** (dựng từ báo cáo BSS, loại khỏi (B)): {', '.join(assumed) or 'không có'}.
Ref {ver_a} là **đợt duyệt 2 giả định theo luật** (manifest `ref_model_map/version={ver_a}`), chưa được
nghiệp vụ xác nhận. Đối chiếu: phạm vi (A) khớp Silver trên đĩa (`silver/_run_info.json`) — đã kiểm.

---

## Slide 4 — Bảng 1: Bốn trở ngại của dữ liệu nghiệp vụ thô · phạm vi (B)

| Trở ngại | Số đo |
|---|---|
| Không có tên xe — chỉ có mã dòng xe nội bộ | {n(B['dims']['dim_model'])} mã trong ref {ver_b} → {B['hieu']} dòng xe → {B['hang']} hãng; ref phủ {p(cover[ver_b]['codes_pct'], 1)}% số mã xuất hiện; {p(100 * B['rules']['MODEL_CODE_UNKNOWN'] / B['bronze'])}% dòng vào `MODEL_CODE_UNKNOWN` |
| Giá thiếu được ghi thành `0` | {n(raw_b['price_zero'])} dòng ({p(100 * raw_b['price_zero'] / raw_b['rows'], 1)}%) |
| Hãng xe, kiểu dáng gần như trống; không có hộp số, số km | hãng {p(raw_b['hang_xe'])}% · kiểu dáng {p(raw_b['ma_kieu_dang'])}% · hộp số 0% |
| Dữ liệu đến theo lô tháng | {raw_b['lots']} lô, {n(raw_b['lot_min'])} – {n(raw_b['lot_max'])} dòng/lô, tổng {n(raw_b['rows'])} dòng |

## Slide 15 — Bảng 10: Những gì nguồn không có · phạm vi (B)

| Trường | Tình trạng |
|---|---|
| Hộp số, số km | Không có cột |
| Kiểu dáng (`ma_kieu_dang`) | {p(raw_b['ma_kieu_dang'])}% có giá trị |
| Hãng xe dạng chữ (`hang_xe`) | {p(raw_b['hang_xe'])}% có giá trị |
| Mã xe khác nhau trong dữ liệu | {n(raw_b['n_codes'])} mã |

## Slide 18 — Bảng 13: Grain và số dòng bảng Silver · phạm vi (A)

| Bảng | Dòng |
|---|---|
| `fact_valuation` | {n(A['fact'])} |
| `dim_model` | {n(A['dims']['dim_model'])} ({A['hieu']} dòng xe, {A['hang']} hãng) |
| `dim_vehicle_type` | {n(A['dims']['dim_vehicle_type'])} |
| `dim_date` | {n(A['dims']['dim_date'])} |
| `quarantine` | {n(A['quarantine'])} |

## Slide 20 — Bảng 15: Tám luật quarantine

{md_table(rule_rows)}
| **Tổng** | **{n(A['quarantine'])}** | 100 | **{n(B['quarantine'])}** |

Đối soát (2): (A) {n(A['bronze'])} = {n(A['fact'])} + {n(A['quarantine'])} · (B) {n(B['bronze'])} = {n(B['fact'])} + {n(B['quarantine'])}.

## Slide 21 — Bảng 16: Ba mart phân tích · phạm vi (A)

| Mart | Dòng |
|---|---|
| `mart_vehicle_price` | {n(len(A['mvp']))} |
| `mart_monthly_trend` | {n(A['mmt_rows'])} |
| `mart_data_quality` | {n(len(A['dq']))} |

## Slide 22 — Bảng ML · phạm vi (A), mốc chia m₀ = {ML_TRAIN_END_MONTH}

| | Tháng | Dòng | Tỷ lệ | Xe cũ | Dòng xe |
|---|---|---|---|---|---|
| train | {tr.thang_duyet.min()} → {tr.thang_duyet.max()} | {n(len(tr))} | {p(100 * len(tr) / len(ml_a), 1)}% | {n((tr.loai_dinh_gia == 'used').sum())} | {tr.hieu_xe.nunique()} |
| test | {te.thang_duyet.min()} → {te.thang_duyet.max()} | {n(len(te))} | {p(100 * len(te) / len(ml_a), 1)}% | {n((te.loai_dinh_gia == 'used').sum())} | {te.hieu_xe.nunique()} |

Dòng xe có trong test nhưng không có trong train: {len(unseen)}{(' (' + ', '.join(sorted(unseen)) + ')') if unseen else ''}.
Hình 9: {figs['h9']}

## Slide 23 — Bảng 18: Số dòng qua từng tầng · phạm vi (A)

| Tầng | Bảng | Dòng |
|---|---|---|
| Landing → Bronze | {len(months_a)}/{len(months_a)} lô qua cổng kiểm tra | {n(A['bronze'])} |
| Silver | `fact_valuation` · `quarantine` | {n(A['fact'])} · {n(A['quarantine'])} |
| Gold | 3 mart BI · bảng ML | {n(len(A['mvp']))} / {n(A['mmt_rows'])} / {n(len(A['dq']))} · {n(len(ml_a))} |

Tỷ lệ quarantine (5): (A) toàn bộ **{p(q(A))}%**, theo tháng {p(dqa.ty_le_quarantine_pct.min())}% – {p(dqa.ty_le_quarantine_pct.max())}% ·
(B) toàn bộ **{p(q(B))}%**, theo tháng {p(B['dq'].ty_le_quarantine_pct.min())}% – {p(B['dq'].ty_le_quarantine_pct.max())}%.

Hình 5: {figs['h5']}

Bảng số của Hình 5 (phạm vi A):

{md_table(dq_tbl)}

## Slide 24 — Phát hiện từ dữ liệu · phạm vi (B)

1. **Tỷ lệ còn lại ρ** (3): {rs['both']}/{n(rs['groups'])} nhóm có đủ giá mới lẫn giá cũ; trung vị ρ = **{p(rs['median'], 3)}**, khoảng {p(rs['min'], 3)} – {p(rs['max'], 3)}.
2. **{rs['gt1']} nhóm có ρ > 1** (bảng dưới).
3. **Chất lượng phụ thuộc bảng tham chiếu**: {p(100 * B['rules']['MODEL_CODE_UNKNOWN'] / B['quarantine'], 0)}% số dòng quarantine là `MODEL_CODE_UNKNOWN`.

{md_table(by_year)}

Hình 6: {figs['h6']}

Nhóm có ρ > 1:

{md_table(gt1_tbl)}

Hình 8 (Q2 — số hợp đồng theo tháng, phạm vi B): {figs['h8']}

## Slide mới — Phiên bản reference data (ARCHITECTURE §4.2) · {len(months_a)} lô

{md_table(ref_tbl)}

Hình 7: {figs['h7']}

Sổ lần chạy `data/control/run_log.csv`:

{md_table(log_tbl)}

---

## Danh mục hình sinh tự động

| Hình | File | Bảng số đi kèm |
|---|---|---|
| Hình 5 — Tỷ lệ quarantine theo tháng × luật (A) | `{figs['h5']}` | Slide 23 |
| Hình 6 — ρ theo đời xe (B) | `{figs['h6']}` | Slide 24 |
| Hình 7 — Quarantine theo tháng, ref {vers[0]} so với {vers[-1]} | `{figs['h7']}` | Slide phiên bản ref |
| Hình 8 — Số hợp đồng theo tháng, mới/cũ (B) | `{figs['h8']}` | — (đếm từ fact) |
| Hình 9 — Train/test theo tháng (A) | `{figs['h9']}` | Slide 22 |
"""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out = REPORT_DIR / "SO_LIEU.md"
    out.write_text(md, encoding="utf-8")
    return out


def main() -> int:
    from src.run_pipeline import setup_logging
    from src.spark import get_spark
    setup_logging()
    spark = get_spark("report")
    try:
        bronze = silver.active_bronze(spark.read.parquet(BRONZE_DIR.as_posix())).cache()
        assumed = assumed_months()
        versions = reference.list_versions(REF_MODEL_MAP_NAME)
        first, latest = versions[0], versions[-1]
        log.info("Ref: %s · lô giả định: %s", versions, assumed)

        per_ref = {}
        for v in versions:                                    # (A) với từng phiên bản ref — cho Hình 7
            per_ref[v] = build(spark, bronze, v, [])
            log.info("(A) ref %s: fact %d · quarantine %d", v, per_ref[v]["fact"], per_ref[v]["quarantine"])
        A = per_ref[latest]
        check_against_disk(A)
        B = build(spark, bronze, first, assumed)
        log.info("(B) ref %s, %d lô: fact %d · quarantine %d", first, len(B["dq"]), B["fact"], B["quarantine"])
    finally:
        spark.stop()

    raw_b = landing_stats(assumed)
    rs = rho_stats(B["mvp"])
    rel = lambda path: path.relative_to(ROOT).as_posix()
    figs = {
        "h5": rel(fig_quarantine_by_month(A["dq"], assumed)),
        "h6": rel(fig_rho_by_year(rs)),
        "h7": rel(fig_ref_versions(per_ref, assumed)),
        "h8": rel(fig_contracts_by_month(B["ml"])),
        "h9": rel(fig_train_test(A["ml"], assumed)),
    }
    run_log = pd.read_csv(RUN_LOG) if RUN_LOG.exists() else pd.DataFrame()
    out = write_markdown(A, B, per_ref, raw_b, rs, assumed, figs, run_log)
    log.info("Số liệu: %s · %d hình trong %s", rel(out), len(figs), rel(FIG_DIR))
    return 0


if __name__ == "__main__":
    sys.exit(main())
