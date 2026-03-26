"""FastMCP Server — Dataverse Inventory Aging Report.

Provides specialized MCP tools for the mserp_tryaiinventoryagingreportentities entity
from the Tiryaki Operations Dataverse environment.
"""

from __future__ import annotations

import asyncio
import os
import structlog
from fastmcp import FastMCP

# ── Auth Disable ───────────────────────────────────────
os.environ.pop("FASTMCP_AUTH_TOKEN", None)

from dataverse_mcp.config import get_settings
from dataverse_mcp.services.formatter import DataFormatter
from dataverse_mcp.services.summarizer import DataSummarizer
from dataverse_mcp.services.response_guard import guard
from dataverse_mcp.services.column_guard import fix_select, fix_filter, fix_group_by, fix_column, ALLOWED_COLUMNS
from dataverse_mcp.client import DataverseClient

logger = structlog.get_logger(__name__)

# ── Latest Date Helper ────────────────────────────────────
async def _ensure_latest_date_filter(filter_query: str = "") -> str:
    """If mserp_headerreportdate is not in the filter, find latest date and add it.
    This ensures we only look at the most recent snapshot as requested.
    """
    if "mserp_headerreportdate" in (filter_query or ""):
        return filter_query or ""
    
    try:
        # Find the latest date
        result = await client.aggregate_table(
            ENTITY_SET, numeric_field="mserp_headerreportdate", agg_type="max"
        )
        max_date = result.get("mserp_headerreportdate_max")
        if not max_date:
            return filter_query or ""
            
        # Format as YYYY-MM-DD
        if isinstance(max_date, str) and "T" in max_date:
            max_date = max_date.split("T")[0]
            
        date_filter = f"mserp_headerreportdate eq {max_date}"
        if filter_query:
            return f"{date_filter} and ({filter_query})"
        return date_filter
    except Exception:
        return filter_query or ""

# ── Robust Search Helper ──────────────────────────────────
def _apply_robust_search(search_field: str, term: str) -> str:
    """Generates a Turkish-aware OData contains() filter with all case variants."""
    if not term:
        return ""

    def tr_upper(s: str) -> str:
        return s.replace("i", "İ").replace("ı", "I").upper()

    def tr_cap(s: str) -> str:
        if not s:
            return s
        return tr_upper(s)[0] + s[1:].lower()

    term = term.strip()
    # Also try Latin I → Turkish İ (e.g. "ISTANBUL" → "İSTANBUL")
    tr_i = term.replace("I", "İ").replace("i", "İ")
    variations = {tr_cap(term), tr_upper(term), term.upper(), term.lower(), term, tr_i, tr_i.upper()}
    parts = [f"contains({search_field}, '{v}')" for v in variations if v]
    return f"({' or '.join(parts)})"

# ── Settings & Dependencies ─────────────────────────────
settings = get_settings()

ENTITY_SET = settings.entity_set_name
ENTITY_LOGICAL = settings.entity_logical_name

mcp = FastMCP(
    name=settings.mcp_server_name,
    instructions=(
        # ── ROLE ──────────────────────────────────────────────
        "You are a Senior Supply-Chain Data Analyst with deep expertise in inventory management. "
        "You have access to the Tiryaki Group's Inventory Aging Report stored in Microsoft Dataverse "
        f"(entity: '{ENTITY_SET}', ~500 000 records). "
        "Always respond in the same language as the user's message (usually Turkish).\n\n"

        # ── FIELD CATALOG (THE ONLY TRUTH) ────────────────────
        "## Field Catalog — THE ONLY COLUMNS THAT EXIST\n"
        "| Concept | Column Name | Notes |\n"
        "|---|---|---|\n"
        "| Product Name | mserp_itemname | Full product name (Turkish text) |\n"
        "| Product Code | mserp_itemid | Short code like 10IQ4112 |\n"
        "| Product Group (Level 2) | mserp_etgproductlevel02name | Ana grup e.g. Tahıl, Bakliyat, Yağlı Tohumlar |\n"
        "| Product Category (Level 3) | mserp_etgproductlevel03name | Alt grup e.g. Buğday, Nohut, Mısır |\n"
        "| Quantity | mserp_qty | Inventory quantity (numeric) |\n"
        "| Total Value (TL) | mserp_amountmst | Total inventory value in company currency (TL). Use for: fiyat, tutar, değer, maliyet, toplam değer |\n"
        "| FIFO Age (days) | mserp_purchfifo | Days since purchase (FIFO) — NOT a monetary field |\n"
        "| Report Date | mserp_headerreportdate | Use for ALL date filtering |\n"
        "| Site / Facility | mserp_inventsitename | e.g. Gaziantep Tesisi, Muş |\n"
        "| Warehouse | mserp_inventlocationname | Sub-location within a site |\n"
        "| Company | mserp_companyname | e.g. MESOPOTAMIA FZE |\n\n"

        # ── TURKISH → COLUMN MAPPING ──────────────────────────
        "## Turkish → Column Mapping\n"
        "When the user says:\n"
        "- 'ürün' / 'malzeme' / 'isim' → use `mserp_itemname`\n"
        "- 'ana grup' / 'ürün grubu' / 'seviye 2' → use `mserp_etgproductlevel02name`\n"
        "- 'alt kategori' / 'kategori' / 'seviye 3' / 'ürün kategorisi' → use `mserp_etgproductlevel03name`\n"
        "- **Category level unknown**: If the user mentions a category name (e.g. 'Tahıl', 'Bakliyat') and you are NOT sure whether it is Level 2 or Level 3, "
        "FIRST call `calculate_inventory_totals(agg_type=count, group_by=mserp_etgproductlevel02name)` to see all Level 2 values. "
        "If the term appears there → use `mserp_etgproductlevel02name`. If not → use `mserp_etgproductlevel03name`.\n"
        "- 'tesis' / 'depo' / 'site' → use `mserp_inventsitename`\n"
        "- 'şirket' / 'firma' → use `mserp_companyname`\n"
        "- **'fiyat' / 'tutar' / 'değer' / 'maliyet' / 'toplam değer'** → use `mserp_amountmst` (total inventory value in TL). NEVER use `mserp_purchfifo` for monetary queries.\n"
        "- **'ortalama yaş' (average age)** → ALWAYS use `calculate_weighted_average` with `mserp_purchfifo` weighted by `mserp_qty`.\n\n"

        # ── FILTERING RULES — NO EXCEPTIONS ──────────────────
        "## Filtering Rules — NO EXCEPTIONS\n"
        "1. **SEARCH BY NAME**: ALWAYS use `contains(column, 'value')` for text fields (`mserp_itemname`, `mserp_inventsitename`, etc.). NEVER use `eq` for text fields — it requires an exact match and will miss records like 'Muş Tesisi' when searching for 'Muş'.\n"
        "2. **NO TECHNICAL IDs**: NEVER use fields ending in 'id' (e.g., `mserp_siteid`) for filtering by text. They do not exist for you.\n"
        "3. **DATE AUTOMATION**: The server automatically filters to the LATEST date. Do NOT add `mserp_headerreportdate` to filters unless a specific past date is requested.\n"
        "4. **DATE RANGES**: NEVER use OData date functions like `month()`, `year()`, `day()` — they are NOT supported by F&O Virtual Entities. "
        "Use ISO date range instead: 'Bu ay' → `mserp_headerreportdate ge 2026-03-01 and mserp_headerreportdate le 2026-03-31`. "
        "'Belirli bir tarih' → `mserp_headerreportdate eq 2026-03-14`.\n"
        "5. **TURKISH CHARS**: Dataverse search is SENSITIVE to Turkish characters. If searching for Muş, use exact 'Muş' or 'MUŞ' in `contains()`. NEVER swap 'Ş' for 'S'.\n"
        "6. **FINANCIAL FIELD**: `mserp_amountmst` = total inventory value in TL (use for: fiyat, tutar, değer, maliyet, toplam değer). `mserp_purchfifo` = FIFO age in DAYS (NOT money). NEVER sum `mserp_purchfifo` for monetary totals.\n\n"

        # ── UNIVERSAL DATA AWARENESS ──────────────────────────
        "## Universal Data Awareness — How you 'see' everything\n"
        "You have access to the ENTIRE latest report (~500k rows) through three lenses:\n"
        "1. **Eagle Eye (Aggregation)**: Use `calculate_inventory_totals` to see the WHOLE report's sums/averages instantly. You are OMNISCIENT here.\n"
        "2. **Searchlight (Filtering)**: Use `search_inventory_aging` to find ANY specific needle in the 500k haystack.\n"
        "3. **Paging (Scrolling)**: Use `query_inventory_aging` with `next_token` to scroll through the report page-by-page. "
        "F&O Virtual Entities do not support `skip`. They ONLY support `next_token`. To see the next page, "
        "pass the token returned in the previous response's footer.\n\n"

        # ── MULTI-STEP ANALYSIS WORKFLOW ──────────────────────
        "## Multi-Step Analysis Workflow\n"
        "For comprehensive insights on the LATEST data, follow this pattern:\n"
        "1. **Aggregate first**: Use `calculate_inventory_totals` for totals — no rows downloaded, instant results.\n"
        "   - 'Tesis bazında stok' → `calculate_inventory_totals(numeric_field=mserp_qty, agg_type=sum, group_by=mserp_inventsitename)`\n"
        "   - 'Ortalama envanter yaşı' → `calculate_weighted_average(value_field=mserp_purchfifo, weight_field=mserp_qty)`\n"
        "   - Çok boyutlu analiz için farklı `group_by` değerleriyle DEFALARCA çağır.\n"
        "2. **Drill-down**: Anomali veya ilgi çeken bir şey varsa `search_inventory_aging` ile detay getir.\n"
        "3. **Paging**: Ham kayıtlar için `query_inventory_aging` kullan; devam için `next_token` parametresini ilet.\n"
        "4. **Multi-metric**: Aynı alan için sum/avg/min/max/count istersen `calculate_multi_metrics` ile tek çağrıda al.\n"
        "5. **Kombinasyon**: `filter_query` ile bir boyutu sabitle, `group_by` ile diğerini analiz et.\n"
    ),
)

# Initialize components
client = DataverseClient(
    dataverse_url=settings.dataverse_url,
    client_id=settings.client_id,
    client_secret=settings.client_secret,
    tenant_id=settings.tenant_id,
)
summarizer = DataSummarizer(max_tokens=settings.summary_max_tokens)
formatter = DataFormatter()


# ═══════════════════════════════════════════════════════════
# MCP TOOLS — Inventory Aging Report
# ═══════════════════════════════════════════════════════════


@mcp.tool()
async def get_inventory_aging_schema() -> str:
    """Envanter Yaşlandırma Raporu tablosunun mevcut kolon listesini ve tip bilgilerini döndürür.
    Hangi field'ların var olduğundan emin değilsen bu tool ile kontrol et.
    Normal sorgu/analiz akışında KULLANMA — yalnızca schema araştırması gerektiğinde kullan.
    """
    try:
        schema = await client.get_table_schema(ENTITY_LOGICAL)
        return guard(formatter.format_schema(schema))
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_inventory_aging_count(filter_query: str = "") -> str:
    """Envanter Yaşlandırma Raporu'ndaki kayıt sayısını döndürür. Kayıt indirmez.

    KULLAN:
    - 'Kaç kayıt var?' veya belirli bir filtreye kaç kayıt düştüğünü öğrenmek için
    - Hızlı saniye sayımı — tüm ~500k kayıt için bile anında döner

    KULLANMA:
    - Toplam miktar/tutar hesabı → calculate_inventory_totals kullan
    - Veri görmek → query_inventory_aging kullan

    Tarih belirtilmezse EN SON rapor tarihindeki kayıt sayısını verir.

    Args:
        filter_query: OData $filter ifadesi (örn. "contains(mserp_inventsitename, 'Gaziantep')").
    """
    try:
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)
        count = await client.get_record_count(ENTITY_SET, filter_query=filter_query)
        msg = f"Inventory Aging Report record count: **{count:,}**"
        if filter_query:
            msg += f"\n- Filter: `{filter_query}`"
        return msg
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_latest_report_date() -> str:
    """Veri setindeki en güncel rapor tarihini döndürür.

    KULLAN:
    - Kullanıcı 'en güncel veri', 'son rapor' veya hangi tarihin mevcut olduğunu sorarsa
    - Belirli bir geçmiş tarih istenmemişse bu tool'u çağırmaya gerek yok —
      server tüm diğer tool'larda tarihi otomatik filtreler.

    KULLANMA:
    - Normal sorgu akışında — server otomatik olarak en son tarihi kullanır, bu tool'u çağırmak gerekmez.

    Dönen tarihi filter_query içinde şu şekilde kullanabilirsin: mserp_headerreportdate eq <tarih>
    """
    try:
        result = await client.aggregate_table(
            ENTITY_SET, numeric_field="mserp_headerreportdate", agg_type="max"
        )
        max_date = result.get("mserp_headerreportdate_max", "Unknown")
        # Extract just the date portion (remove time)
        if isinstance(max_date, str) and "T" in max_date:
            max_date = max_date.split("T")[0]
        return f"Latest report date: **{max_date}**\nUse this in your filters: `mserp_headerreportdate eq {max_date}`"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def query_inventory_aging(
    select: str = "",
    filter_query: str = "",
    orderby: str = "",
    top: int = 50,
    next_token: str = "",
) -> str:
    """Envanter Yaşlandırma Raporu'ndan ham kayıtları listeler. Toplam/aggregation YAPMAZ.

    KULLAN:
    - Belirli kayıtların detaylarını görmek (ürün, miktar, yaş, tesis, depo)
    - Filtre uygulayıp satırları sayfalar halinde incelemek
    - select ile istenen kolonları seçip sayfalama yapmak
    - 'En yaşlı stok', 'En çok miktarlı ürün' → orderby ile sırala

    KULLANMA — bunlar için başka tool kullan:
    - Toplam/ortalama/gruplama → calculate_inventory_totals kullan
    - Ortalama yaş (ağırlıklı) → calculate_weighted_average kullan
    - Anahtar kelime araması → search_inventory_aging kullan
    - sum/avg/min/max/count birlikte → calculate_multi_metrics kullan

    KRİTİK — Sayfalama:
    - F&O Sanal Entity'leri `skip` desteklemez; yalnızca `next_token` (skiptoken) kullan.
    - Sonraki sayfayı görmek için önceki yanıttaki token'ı next_token parametresine ver.

    Args:
        select: Virgülle ayrılmış kolon listesi (Field Catalog'a bakınız).
        filter_query: OData $filter ifadesi (örn. "contains(mserp_itemname, 'BUĞDAY')").
        orderby: OData $orderby ifadesi (örn. "mserp_qty desc", "mserp_purchfifo desc").
        top: Sayfa başına maksimum kayıt sayısı (varsayılan: 50, maks: 500).
        next_token: Bir sonraki sayfayı getirmek için önceki yanıttaki sayfalama token'ı.
    """
    try:
        if top > 500:
            top = 500
        
        # Auto-correct hallucinated column names
        select = fix_select(select)
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)
        
        # Call client with skiptoken support
        result_dict = await client.query_table(
            ENTITY_SET,
            select=select or None,
            filter_query=filter_query or None,
            orderby=orderby or None,
            top=top,
            next_link=next_token or None,
        )
        
        records = result_dict.get("value", [])
        next_link = result_dict.get("@odata.nextLink")
        
        columns = [c.strip() for c in select.split(",")] if select else None
        result_table = formatter.format_records_table(records, columns=columns)
        
        # Build response with completeness & pagination info
        header = f"**Inventory Aging Report** — Showing {len(records)} records."
        
        if next_link:
            header += f"\n\n> **Daha fazla kayıt var.** Bir sonraki sayfayı görmek için `next_token` parametresine şu değeri yapıştırın:\n> `{next_link}`"
        
        if not next_token:
             header += "\n> Toplam/ortalama hesaplamaları için `calculate_inventory_totals` kullanmanızı öneririm."

        return guard(f"{header}\n\n{result_table}")
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def search_inventory_aging(
    search_field: str,
    search_term: str,
    select: str = "",
    top: int = 20,
    next_token: str = "",
) -> str:
    """Envanter Yaşlandırma Raporu'nda metin araması yapar. Türkçe karakterleri otomatik genişletir.

    KULLAN:
    - Ürün adıyla arama: search_field=mserp_itemname, search_term='Buğday'
    - Tesis adıyla arama: search_field=mserp_inventsitename, search_term='Gaziantep'
    - Şirket adıyla arama: search_field=mserp_companyname, search_term='Mesopotamia'
    - Kategori araması: search_field=mserp_etgproductlevel03name, search_term='Mısır'

    KULLANMA:
    - Toplam/gruplama → calculate_inventory_totals kullan
    - Zaten filter_query biliyorsan → query_inventory_aging kullan

    KRİTİK: ALWAYS use name columns, NEVER id columns:
    - mserp_inventsitename ✓  (mserp_siteid ✗)
    - mserp_companyname ✓     (mserp_companyid ✗)
    - mserp_itemname ✓        (mserp_itemid ✗)

    Args:
        search_field: Aranacak kolon (örn. 'mserp_itemname', 'mserp_inventsitename').
        search_term: Arama kelimesi — Türkçe büyük/küçük harf otomatik işlenir.
        select: Virgülle ayrılmış döndürülecek kolonlar.
        top: Maks sonuç sayısı (varsayılan: 20).
        next_token: Sonraki sayfa için sayfalama token'ı.
    """
    try:
        # Auto-correct and enforce date
        search_field = fix_column(search_field)
        select = fix_select(select)
        
        # Apply robust search filter
        robust_filter = _apply_robust_search(search_field, search_term)
        base_filter = await _ensure_latest_date_filter("")
        
        if base_filter:
            final_filter = f"({base_filter}) and {robust_filter}"
        else:
            final_filter = robust_filter
        
        result_dict = await client.query_table(
            ENTITY_SET,
            select=select or None,
            filter_query=final_filter,
            top=top,
            next_link=next_token or None
        )
        records = result_dict.get("value", [])
        next_link = result_dict.get("@odata.nextLink")
        
        result_table = formatter.format_records_table(records)
        
        header = f"**Search Results** — Found {len(records)} records where '{search_field}' contains '{search_term}'"
        if next_link:
            header += f"\n\n> **Daha fazla sonuç var.** Devamı için `next_token` parametresini kullanın:\n> `{next_link}`"
            
        return guard(f"{header}\n\n{result_table}")
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_inventory_aging_record(record_id: str, select: str = "") -> str:
    """GUID ile tek bir Envanter Yaşlandırma kaydını getirir.

    KULLAN:
    - Önceki bir sorgudan dönen kaydın tam detayını görmek istediğinde

    KULLANMA:
    - Liste/arama işlemleri → query_inventory_aging veya search_inventory_aging kullan
    - GUID'i bilmiyorsan bu tool işe yaramaz

    Args:
        record_id: Kaydın GUID değeri (örn. '3fa85f64-5717-4562-b3fc-2c963f66afa6').
        select: Döndürülecek virgülle ayrılmış kolonlar. Boş bırakılırsa tüm kolonlar döner.
    """
    try:
        record = await client.get_record(
            ENTITY_SET, record_id, select=select or None,
        )
        return formatter.format_record(record, table_name="Inventory Aging Report")
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def summarize_inventory_aging(
    select: str = "mserp_qty,mserp_itemname,mserp_headerreportdate",
    filter_query: str = "",
    top: int = 2000,
    sample_size: int = 5,
) -> str:
    """Kayıtlardan örnekleme yaparak istatistiksel özet üretir (maks. 5000 kayıt).

    KULLAN:
    - Veri dağılımını, örüntüleri ve örnek kayıtları birlikte görmek istediğinde
    - 'Nasıl bir veri var?' sorusunu yanıtlamak için keşif amaçlı

    KULLANMA — bunlar için daha doğru tool var:
    - Kesin toplam/ortalama hesabı → calculate_inventory_totals kullan (tüm veriyi işler, bu sadece örnek alır)
    - Ağırlıklı ortalama yaş → calculate_weighted_average kullan
    - Ham kayıt listesi → query_inventory_aging kullan

    KRİTİK: Bu tool örnekleme yapar, tüm veriyi işlemez. Doğru iş kararları için
    calculate_inventory_totals kullan.

    Args:
        select: Analiz edilecek virgülle ayrılmış kolonlar (örn. 'mserp_qty,mserp_itemname').
        filter_query: Örnekleme öncesi veriyi daraltmak için OData $filter ifadesi.
        top: Örnek boyutu (varsayılan: 2000, maks: 5000).
        sample_size: Gösterilecek örnek kayıt sayısı (varsayılan: 5).
    """
    try:
        # Cap at 5000 for safety but allow deep analysis
        actual_top = min(top, 5000)
        
        # Auto-correct and enforce date
        select = fix_select(select)
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)
        
        result_dict = await client.query_table(
            ENTITY_SET, 
            select=select or None, 
            filter_query=filter_query or None, 
            fetch_all=True,
            max_records=actual_top
        )
        records = result_dict.get("value", [])
        
        key_fields = [c.strip() for c in select.split(",")] if select else None
        result = summarizer.summarize_records(
            records, table_name="Inventory Aging Report",
            sample_size=sample_size, key_fields=key_fields,
        )
        return guard(result)
    except Exception as e:
        return f"Error during large-scale summary: {e}"


@mcp.tool()
async def calculate_inventory_totals(
    numeric_field: str = "",
    agg_type: str = "sum",
    group_by: str = "",
    filter_query: str = "",
    top_n: int = 50,
) -> str:
    """TÜM veri seti (~500k kayıt) üzerinde sunucu taraflı agregasyon yapar. Kayıt indirmez.

    KULLAN — bu soruları bu tool ile yanıtla:
    - 'Toplam stok miktarı' → numeric_field=mserp_qty, agg_type=sum
    - 'Tesis bazında stok kırılımı' → agg_type=sum, group_by=mserp_inventsitename
    - 'Şirket bazında ortalama yaş' → numeric_field=mserp_purchfifo, agg_type=average, group_by=mserp_companyname
    - 'Kaç farklı ürün var?' → agg_type=count, group_by=mserp_itemname
    - 'Ana grup (Seviye 2) bazında toplam miktar' → group_by=mserp_etgproductlevel02name
    - 'Alt kategori (Seviye 3) bazında toplam miktar' → group_by=mserp_etgproductlevel03name
    - 'Tahıl grubunun toplam değeri (TL)' → filter_query="contains(mserp_etgproductlevel02name,'Tah')", numeric_field=mserp_amountmst
    - 'Tüm grupların toplam değeri' → numeric_field=mserp_amountmst, agg_type=sum, group_by=mserp_etgproductlevel02name
    - Çok boyutlu analiz için farklı group_by değerleriyle DEFALARCA çağır.

    KULLANMA:
    - Ağırlıklı ortalama yaş hesabı → calculate_weighted_average kullan (aritmetik ortalama yanıltıcı olabilir)
    - sum/avg/min/max/count birlikte → calculate_multi_metrics kullan (tek çağrı, daha hızlı)
    - Ham kayıt görmek → query_inventory_aging kullan

    KRİTİK — group_by'a tarih field'ı YAZMA:
    - group_by parametresine mserp_headerreportdate YAZMA — 400 hatası verir.
    - Tarih filtresi için filter_query parametresini kullan.

    Args:
        numeric_field: Agregasyon yapılacak kolon. Miktar: 'mserp_qty'. Toplam değer (TL): 'mserp_amountmst'. FIFO yaş (gün): 'mserp_purchfifo'. Sayım için boş bırak.
        agg_type: 'sum', 'average', 'min', 'max' veya 'count'.
        group_by: Gruplama kolonu (örn. 'mserp_inventsitename', 'mserp_companyname'). ASLA tarih field'ı yazma.
        filter_query: Kapsamı daraltmak için OData $filter (örn. "contains(mserp_companyname, 'MESQ')").
        top_n: Döndürülecek maks grup sayısı, değere göre azalan sırada (varsayılan: 50).
    """
    try:
        # Auto-correct hallucinated column names
        if numeric_field:
            fixed = fix_column(numeric_field)
            if fixed is None:
                return (
                    f"COLUMN NOT FOUND: '{numeric_field}' is not a valid column. "
                    f"Available numeric fields: mserp_qty (quantity), mserp_amountmst (total value in TL), mserp_purchfifo (FIFO age in days). "
                    f"For financial value/price/cost queries use numeric_field='mserp_amountmst'."
                )
            numeric_field = fixed
        group_by = fix_group_by(group_by)
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)

        result = await client.aggregate_table(
            ENTITY_SET, numeric_field, agg_type,
            filter_query=filter_query, group_by=group_by,
        )
        
        # Determine alias based on agg type
        if agg_type.lower() == "count":
            alias = "record_count"
            label = "Record Count"
        else:
            alias = f"{numeric_field}_{agg_type}"
            label = f"{numeric_field} ({agg_type})"
        
        header = f"### Server-Side Aggregation (Query Pushdown — no rows downloaded)"
        
        if group_by and isinstance(result, list):
            # Sort by aggregate value descending and cap at top_n
            total_groups = len(result)
            result.sort(key=lambda r: r.get(alias, 0), reverse=True)
            display_result = result[:top_n]
            
            group_cols = [c.strip() for c in group_by.split(",")]
            col_headers = " | ".join(group_cols) + f" | {label}"
            col_seps = " | ".join(["---"] * len(group_cols)) + " | ---"
            lines = [header, f"\n| {col_headers} |", f"| {col_seps} |"]
            for row in display_result:
                group_vals = " | ".join(str(row.get(c, "N/A")) for c in group_cols)
                agg_val = row.get(alias, 0)
                lines.append(f"| {group_vals} | {agg_val:,.2f} |")
            
            if total_groups > top_n:
                lines.append(f"\n> *...ve {total_groups - top_n} grup daha (toplam {total_groups} grup). Daha dar bir filtre kullanın.*")
            
            return guard("\n".join(lines))
        else:
            # Single result
            value = result.get(alias, 0) if isinstance(result, dict) else 0
            detail = f"- **{label}**: `{value:,.2f}`"
            if filter_query:
                detail += f"\n- Filter: `{filter_query}`"
            return f"{header}\n{detail}"
    except Exception as e:
        return f"Error: Server-side aggregation failed. {e}"


@mcp.tool()
async def calculate_multi_metrics(
    numeric_field: str,
    filter_query: str = "",
) -> str:
    """Bir alan için TOPLAM, ORTALAMA, MİN, MAKS ve SAYI değerlerini TEK çağrıda paralel hesaplar.

    KULLAN:
    - 'mserp_qty hakkında her şeyi söyle' → tek çağrıda 5 metrik birden
    - 'Stok miktarının dağılımı nedir?' → sum + avg + min + max + count
    - Birden fazla istatistik gerektiğinde calculate_inventory_totals'ı 5 kez çağırmak yerine bunu kullan

    KULLANMA:
    - Sadece tek bir metrik gerekiyorsa → calculate_inventory_totals kullan
    - Gruplama/kırılım gerekiyorsa → calculate_inventory_totals kullan (bu tool group_by desteklemez)
    - Ağırlıklı ortalama yaş → calculate_weighted_average kullan

    Args:
        numeric_field: Analiz edilecek kolon. Miktar: 'mserp_qty'. Toplam değer (TL): 'mserp_amountmst'. FIFO yaş (gün): 'mserp_purchfifo'.
        filter_query: Kapsamı daraltmak için OData $filter ifadesi.
    """
    try:
        fixed = fix_column(numeric_field)
        if fixed is None:
            return (
                f"COLUMN NOT FOUND: '{numeric_field}' is not a valid column. "
                f"Available numeric fields: mserp_qty (quantity), mserp_amountmst (total value in TL), mserp_purchfifo (FIFO age in days). "
                f"For financial value/price/cost queries use numeric_field='mserp_amountmst'."
            )
        numeric_field = fixed
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)

        agg_types = ["sum", "average", "min", "max", "count"]
        tasks = [
            client.aggregate_table(
                ENTITY_SET,
                numeric_field=numeric_field if agg != "count" else "",
                agg_type=agg,
                filter_query=filter_query,
            )
            for agg in agg_types
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        lines = [
            f"### Multi-Metric Analysis: `{numeric_field}`",
            "*All metrics calculated in a SINGLE parallel call for the latest date*\n",
        ]
        if filter_query:
            lines.append(f"- **Filter:** `{filter_query}`\n")

        labels = {
            "sum": "Toplam (Sum)",
            "average": "Ortalama (Average)",
            "min": "Minimum",
            "max": "Maximum",
            "count": "Kayit Sayisi (Count)",
        }

        for agg, result in zip(agg_types, results):
            if isinstance(result, Exception):
                lines.append(f"- **{labels[agg]}**: Error")
                continue
            alias = "record_count" if agg == "count" else f"{numeric_field}_{agg}"
            value = result.get(alias, 0) if isinstance(result, dict) else 0
            lines.append(f"- **{labels[agg]}**: `{value:,.2f}`")

        return "\n".join(lines)
    except Exception as e:
        return f"Error: Multi-metric calculation failed. {e}"


@mcp.tool()
async def calculate_weighted_average(
    value_field: str = "mserp_purchfifo",
    weight_field: str = "mserp_qty",
    group_by: str = "",
    filter_query: str = "",
) -> str:
    """TÜM veri seti üzerinde AĞIRLIKLI ORTALAMA hesaplar. Kayıt indirmez.

    KULLAN — envanter yaşı soruları için BİRİNCİL ARAÇ:
    - 'Ortalama envanter yaşı nedir?' → varsayılan parametrelerle çağır (mserp_purchfifo / mserp_qty)
    - 'Tesis bazında ortalama yaş' → group_by=mserp_inventsitename
    - 'Şirket bazında ortalama yaş' → group_by=mserp_companyname
    - 'Buğdayın ortalama yaşı' → filter_query ile filtrele

    KULLANMA:
    - Miktar toplamı → calculate_inventory_totals kullan
    - Ham kayıt görmek → query_inventory_aging kullan

    KRİTİK: 'Ortalama yaş' sorularında ASLA calculate_inventory_totals(agg_type=average) kullanma —
    aritmetik ortalama yanlış sonuç verir. Miktar (mserp_qty) ile ağırlıklandırılmış ortalama doğrudur.

    Args:
        value_field: Ortalaması alınacak kolon (varsayılan: 'mserp_purchfifo' — envanter yaşı gün cinsinden).
        weight_field: Ağırlık kolonu (varsayılan: 'mserp_qty' — stok miktarı).
        group_by: Gruplama kolonu (örn. 'mserp_companyname', 'mserp_inventsitename'). İsteğe bağlı.
        filter_query: Kapsamı daraltmak için OData $filter ifadesi.
    """
    try:
        fixed_val = fix_column(value_field)
        if fixed_val is None:
            return (
                f"COLUMN NOT FOUND: '{value_field}' is not a valid column. "
                f"Available numeric fields: mserp_qty (quantity), mserp_purchfifo (FIFO cost/value)."
            )
        value_field = fixed_val
        fixed_wt = fix_column(weight_field)
        if fixed_wt is None:
            return (
                f"COLUMN NOT FOUND: '{weight_field}' is not a valid weight column. "
                f"Use mserp_qty for quantity-based weighting."
            )
        weight_field = fixed_wt
        group_by = fix_group_by(group_by)
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)

        result = await client.calculate_weighted_average(
            ENTITY_SET,
            value_field=value_field,
            weight_field=weight_field,
            filter_query=filter_query,
            group_by=group_by
        )
        
        header = f"### Weighted Average Analysis"
        sub_header = f"- **Target Field:** `{value_field}`\n- **Weight Field:** `{weight_field}`"
        
        if group_by and isinstance(result, list):
            # Sort by weighted average descending
            result.sort(key=lambda r: r.get(f"{value_field}_weighted_avg", 0), reverse=True)
            
            lines = [header, sub_header, f"\n| {group_by} | Weighted Average | Total Weight |", "|---|---|---|"]
            for row in result:
                g_val = row.get(group_by, "N/A")
                avg = row.get(f"{value_field}_weighted_avg", 0)
                weight = row.get("total_weight", 0)
                lines.append(f"| {g_val} | **{avg:.2f}** | {weight:,.2f} |")
            return "\n".join(lines)
        else:
            # Single value
            avg_val = result if isinstance(result, (int, float)) else 0
            return f"{header}\n{sub_header}\n\n**Calculated Weighted Average: {avg_val:.2f}**"

    except Exception as e:
        return f"Error calculating weighted average: {e}"


@mcp.tool()
def whoami(user_email: str) -> dict:
    """
    Kullanıcının kim olduğunu öğrenmek için bu tool'u çağır.
    Kullanıcı 'kim olduğumu söyle', 'emailim nedir', 'ben kimim' gibi sorular sorarsa MUTLAKA bu tool'u kullan.
    user_email: Kullanıcıya SORMA. System prompt'taki 'Kullanıcı emaili' değerini kullan.
    """
    logger.info("whoami called", user_email=user_email)
    return {"received_email": user_email}


# ═══════════════════════════════════════════════════════════
# MCP RESOURCES
# ═══════════════════════════════════════════════════════════


@mcp.resource("dataverse://inventory-aging/schema")
async def resource_inventory_aging_schema() -> str:
    """Schema of the Inventory Aging Report entity."""
    schema = await client.get_table_schema(ENTITY_LOGICAL)
    return formatter.format_schema(schema)


# ═══════════════════════════════════════════════════════════
# MCP PROMPTS
# ═══════════════════════════════════════════════════════════


@mcp.prompt()
def analyze_inventory_aging(analysis_goal: str = "genel analiz") -> str:
    """Prompt template for analyzing inventory aging data."""
    return f"""Lütfen Envanter Yaşlandırma Raporu (Inventory Aging Report) verilerini analiz edin.

Analiz Hedefi: {analysis_goal}

Adımlar:
1. `calculate_inventory_totals` ile şirket bazlı toplam miktarları hesaplayın (group_by='mserp_companyname').
2. `calculate_inventory_totals` ile tesis bazlı dağılımı çıkarın (group_by='mserp_inventsitename').
3. `calculate_inventory_totals` ile ürün kategorisi kırılımını alın (group_by='mserp_etgproductlevel03name').
4. Gerekirse `query_inventory_aging` ile ham kayıt örnekleri getirin.
5. Tüm sonuçları birleştirerek kapsamlı içgörüler ve öneriler sunun.

Yanıtlarınızı Türkçe verin."""


@mcp.prompt()
def filter_aging_items(min_days: str = "90", field_name: str = "mserp_purchfifo") -> str:
    """Prompt template for filtering aged inventory items."""
    return f"""Envanter Yaşlandırma Raporundan {min_days} günden eski kalemleri analiz edin.

Adımlar:
1. `calculate_inventory_totals` ile `{field_name} gt {min_days}` filtresi uygulayarak toplam kayıt sayısını bulun.
2. `calculate_inventory_totals` ile aynı filtreyi uygulayıp şirket bazında kırılım çıkarın (group_by='mserp_companyname').
3. `query_inventory_aging` ile en kritik 10 kalemi getirin (orderby='{field_name} desc', top=10).
4. Sonuçları ve tavsiyeleri özetleyin.

Yanıtlarınızı Türkçe verin."""

