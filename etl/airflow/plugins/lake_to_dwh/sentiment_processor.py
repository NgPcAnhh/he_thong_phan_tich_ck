import csv
import io
import logging
import re
import traceback
from contextlib import closing
from typing import Dict, List, Optional
import pandas as pd
from psycopg2.extras import execute_values
from lake_to_dwh.utils import get_postgres_connection

logger = logging.getLogger(__name__)

# ==========================================
# 1. TỪ ĐIỂN TỪ KHÓA CHUNG
# ==========================================
BASE_SECTOR_DICT = {
    "Ngân hàng": [
        "banking", "commercial bank", "ngân hàng thương mại", "nhtm", "vay mua nhà",
        "vay tiêu dùng", "vay doanh nghiệp", "tín dụng bán lẻ", "tín dụng doanh nghiệp",
        "tăng trưởng tín dụng", "hạn mức tín dụng", "nợ quá hạn", "nợ tái cơ cấu",
        "trích lập dự phòng", "dự phòng rủi ro", "chi phí tín dụng", "nim",
        "thu nhập lãi thuần", "nii", "casa ratio", "ldr", "car", "basel ii",
        "basel iii", "thanh khoản hệ thống", "liên ngân hàng", "lãi suất liên ngân hàng",
        "tỷ giá", "ngoại hối", "usd/vnd", "bancassurance", "ngân hàng số",
        "mobile banking", "internet banking", "thẻ tín dụng", "thẻ ghi nợ",
        "thanh toán không tiền mặt", "ví điện tử", "core banking"
    ],

    "Bất động sản": [
        "real estate", "bds", "nhà đất", "môi giới bất động sản", "chủ đầu tư",
        "phát triển dự án", "pháp lý dự án", "giấy phép xây dựng", "mở bán",
        "bàn giao nhà", "booking căn hộ", "hấp thụ căn hộ", "tỷ lệ hấp thụ",
        "giá bán sơ cấp", "giá bán thứ cấp", "đấu giá đất", "tiền sử dụng đất",
        "đất thương mại dịch vụ", "đất ở đô thị", "đất khu công nghiệp",
        "bất động sản khu công nghiệp", "khu đô thị", "đại đô thị", "nhà phố",
        "văn phòng cho thuê", "mặt bằng bán lẻ", "tòa nhà văn phòng",
        "lấp đầy", "tỷ lệ lấp đầy", "giá thuê", "bất động sản nghỉ dưỡng",
        "second home", "timeshare", "proptech"
    ],

    "Dịch vụ tài chính": [
        "financial services", "securities", "brokerage", "công ty chứng khoán",
        "môi giới chứng khoán", "ngân hàng đầu tư", "investment banking",
        "quản lý tài sản", "asset management", "quản lý quỹ", "fund management",
        "aum", "nav", "chứng chỉ quỹ", "quỹ mở", "quỹ đóng", "quỹ trái phiếu",
        "quỹ cổ phiếu", "ipo", "niêm yết", "hủy niêm yết", "phát hành riêng lẻ",
        "phát hành thêm", "cổ tức", "cổ tức tiền mặt", "cổ tức cổ phiếu",
        "mua cổ phiếu quỹ", "repo", "margin lending", "call margin",
        "force sell", "tự doanh chứng khoán", "m&a", "tư vấn tài chính",
        "bảo lãnh phát hành", "lưu ký chứng khoán", "vps", "ssi", "vndirect"
    ],

    "Xây dựng và Vật liệu": [
        "construction", "building materials", "hạ tầng", "đầu tư hạ tầng",
        "dự án hạ tầng", "cao tốc bắc nam", "đường vành đai", "metro",
        "sân bay long thành", "cầu đường", "hầm đường bộ", "nút giao",
        "tổng thầu", "epc", "m&e", "xây lắp", "giá trị hợp đồng",
        "backlog", "khối lượng thi công", "giải phóng mặt bằng",
        "bê tông", "bê tông thương phẩm", "cọc bê tông", "gạch",
        "kính xây dựng", "ống nhựa", "ống thép", "thép xây dựng",
        "thép thanh", "thép hình", "quặng sắt", "giá thép", "clinker",
        "trạm trộn", "nhà máy xi măng"
    ],

    "Thực phẩm và đồ uống": [
        "food and beverage", "f&b", "hàng tiêu dùng nhanh", "hàng tiêu dùng thiết yếu",
        "consumer staples", "nông sản", "chăn nuôi", "gia súc", "gia cầm",
        "heo hơi", "giá heo hơi", "sản phẩm sữa", "sữa bột", "sữa tươi",
        "bánh kẹo", "mì ăn liền", "gia vị", "nước mắm", "đồ hộp",
        "thực phẩm đông lạnh", "thực phẩm chế biến", "chuỗi f&b", "nhà hàng",
        "cà phê", "hạt điều", "hồ tiêu", "sắn", "ngô", "đậu tương",
        "xuất khẩu gạo", "xuất khẩu cà phê", "xuất khẩu thủy sản",
        "kim ngạch xuất khẩu nông sản", "đơn hàng xuất khẩu", "vùng nuôi",
        "asp", "sản lượng tiêu thụ"
    ],

    "Dầu khí": [
        "oil and gas", "petroleum", "upstream", "midstream", "downstream",
        "e&p", "thăm dò khai thác", "dịch vụ dầu khí", "dịch vụ khoan",
        "khoan dầu", "mỏ dầu", "mỏ khí", "đường ống khí", "vận chuyển khí",
        "chế biến khí", "khí thiên nhiên", "lpg", "condensate", "dầu diesel",
        "ron 95", "e5 ron 92", "giá xăng", "giá bán lẻ xăng dầu",
        "quỹ bình ổn xăng dầu", "crack spread", "biên lọc dầu",
        "nhà máy lọc dầu", "lọc dầu dung quất", "lọc hóa dầu nghi sơn",
        "pvn", "petrovietnam", "opec+", "tồn kho dầu", "sản lượng khai thác"
    ],

    "Bán lẻ": [
        "retail", "modern trade", "general trade", "mt", "gt",
        "điện máy", "cửa hàng điện máy", "điện thoại di động", "laptop",
        "hàng điện tử", "hàng gia dụng", "bán lẻ dược phẩm", "chuỗi nhà thuốc",
        "minimart", "hypermarket", "supermarket", "department store",
        "marketplace", "online retail", "omnichannel", "o2o",
        "same store sales", "sss", "doanh số cửa hàng", "doanh số cùng cửa hàng",
        "lưu lượng khách", "basket size", "giỏ hàng", "tỷ lệ chuyển đổi",
        "khuyến mãi", "hàng tồn kho", "inventory", "mở mới cửa hàng",
        "đóng cửa hàng", "độ phủ cửa hàng"
    ],

    "Công nghệ Thông tin": [
        "information technology", "software outsourcing", "outsourcing",
        "saas", "paas", "iaas", "big data", "data center", "trung tâm dữ liệu",
        "machine learning", "deep learning", "generative ai", "genai",
        "blockchain", "fintech", "edtech", "healthtech", "iot",
        "internet of things", "erp", "crm", "sap", "oracle", "cybersecurity",
        "an ninh mạng", "bảo mật dữ liệu", "digital transformation",
        "tự động hóa", "automation", "rpa", "chip", "semiconductor",
        "thiết kế chip", "fabless", "foundry", "kiểm thử phần mềm",
        "dịch vụ cntt", "xuất khẩu phần mềm"
    ],

    "Điện, nước & xăng dầu khí đốt": [
        "utilities", "utility", "power", "electricity", "renewable energy",
        "solar power", "wind power", "điện khí", "điện sinh khối",
        "điện rác", "lng-to-power", "pdp8", "quy hoạch điện viii",
        "ppa", "hợp đồng mua bán điện", "giá phát điện", "giá bán điện",
        "sản lượng điện", "phát điện", "truyền tải", "phân phối điện",
        "công suất lắp đặt", "công suất phát điện", "evn", "genco",
        "pc1", "nước sạch", "water supply", "xử lý nước thải",
        "nhà máy nước", "đường ống nước", "tỷ lệ thất thoát nước",
        "khí hóa lỏng", "phân phối khí", "khí thấp áp"
    ],

    "Du lịch và Giải trí": [
        "tourism", "hospitality", "aviation", "airline", "hãng hàng không",
        "vận tải hàng không", "sân golf", "khu vui chơi", "theme park",
        "công viên giải trí", "duty free", "miễn thị thực", "visa",
        "e-visa", "khách quốc tế", "khách nội địa", "lượng khách",
        "công suất phòng", "giá phòng", "adr", "revpar", "mice",
        "hội nghị hội thảo", "ota", "đại lý du lịch trực tuyến",
        "homestay", "villa nghỉ dưỡng", "tàu du lịch", "cruise",
        "dịch vụ lưu trú", "đặt phòng", "khu nghỉ dưỡng", "vui chơi giải trí"
    ],

    "Bảo hiểm": [
        "insurance", "life insurance", "non-life insurance", "premium",
        "gross written premium", "gwp", "phí bảo hiểm gốc", "phí tái bảo hiểm",
        "hoa hồng bảo hiểm", "đại lý bảo hiểm", "môi giới bảo hiểm",
        "bancassurance", "hợp đồng bảo hiểm", "khai thác mới",
        "ape", "vnb", "combined ratio", "loss ratio", "tỷ lệ bồi thường",
        "dự phòng nghiệp vụ", "biên khả năng thanh toán", "chi trả quyền lợi",
        "bảo hiểm sức khỏe", "bảo hiểm xe cơ giới", "bảo hiểm tài sản",
        "bảo hiểm cháy nổ", "bảo hiểm hàng hải", "bảo hiểm trách nhiệm"
    ],

    "Y tế": [
        "healthcare", "pharma", "pharmaceutical", "dược", "dược liệu",
        "thuốc generic", "biệt dược", "otc", "etc", "kênh otc",
        "kênh etc", "đấu thầu thuốc", "gói thầu thuốc", "api dược",
        "nguyên liệu dược", "vật tư y tế", "xét nghiệm", "chẩn đoán hình ảnh",
        "telemedicine", "y tế số", "chăm sóc sức khỏe", "y tế tư nhân",
        "bệnh viện tư", "chuỗi phòng khám", "chuỗi nhà thuốc",
        "tiêm chủng", "vaccine", "sinh phẩm", "thiết bị chẩn đoán",
        "bảo hiểm y tế", "bhyt", "khám bệnh", "điều trị"
    ],

    "Hóa chất": [
        "chemical", "chemicals", "fertilizer", "phân đạm", "đạm ure",
        "dap", "kali", "amoniac", "ammonia", "acid sulfuric",
        "sulfuric acid", "xút clo", "soda ash", "hóa chất cơ bản",
        "hóa chất công nghiệp", "hóa chất nông nghiệp", "thuốc bảo vệ thực vật",
        "pesticide", "cao su thiên nhiên", "latex", "mủ latex",
        "radial tire", "lốp radial", "carbon black", "pvc", "pe", "pp",
        "pet resin", "hạt nhựa nguyên sinh", "nhựa tái sinh",
        "petrochemical", "hóa dầu", "phốt pho vàng", "phosphorus"
    ],

    "Viễn thông": [
        "telecom", "telecommunications", "mobile network", "mạng viễn thông",
        "viễn thông di động", "fiber", "ftth", "broadband", "isp",
        "nhà mạng", "sim", "esim", "thuê bao trả trước", "thuê bao trả sau",
        "arpu", "doanh thu dịch vụ viễn thông", "roaming", "chuyển mạng giữ số",
        "bts", "trạm bts", "hạ tầng viễn thông", "cáp biển", "tuyến cáp biển",
        "aag", "apg", "internet băng rộng", "dịch vụ dữ liệu",
        "data mobile", "mobile money", "vệ tinh", "leo satellite"
    ],

    "Ô tô và phụ tùng": [
        "automotive", "vehicle", "car", "motorbike", "xe con", "xe tải",
        "xe buýt", "xe thương mại", "suv", "sedan", "pickup", "mpv",
        "xe hybrid", "ô tô điện", "pin xe điện", "battery", "trạm sạc",
        "sạc nhanh", "charging station", "ắc quy", "lốp ô tô",
        "phụ tùng ô tô", "linh kiện ô tô", "aftermarket", "đăng kiểm",
        "thuế trước bạ", "lệ phí trước bạ", "nhập khẩu ô tô",
        "sản xuất ô tô", "lắp ráp ô tô", "ckd", "cbu", "doanh số ô tô",
        "tiêu thụ ô tô"
    ],

    "Truyền thông": [
        "media", "advertising", "digital marketing", "content marketing",
        "truyền thông số", "quảng cáo số", "quảng cáo trực tuyến",
        "quảng cáo ngoài trời", "ooh", "dooh", "rating", "ott",
        "streaming", "bản quyền truyền hình", "sản xuất nội dung",
        "content creator", "kol", "kols", "koc", "influencer",
        "social media", "facebook ads", "google ads", "tiktok ads",
        "booking quảng cáo", "chi tiêu quảng cáo", "ngân sách marketing",
        "thương hiệu", "brand awareness", "public relations", "quan hệ công chúng"
    ],

    "Hàng & Dịch vụ Công nghiệp": [
        "industrial goods", "industrial services", "seaport", "shipping",
        "freight forwarding", "giao nhận", "kho vận", "warehouse",
        "warehousing", "kho lạnh", "cold chain", "last mile",
        "chuyển phát nhanh", "express delivery", "vận tải đường bộ",
        "vận tải đường sắt", "vận tải thủy", "vận tải hàng không",
        "air cargo", "icd", "depot", "cảng cạn", "teu", "throughput",
        "sản lượng container", "dry bulk", "bulk carrier", "tàu hàng rời",
        "shipping line", "cước container", "giá cước logistics",
        "dịch vụ cảng", "xếp dỡ container", "chuỗi lạnh", "máy móc công nghiệp"
    ],

    "Tài nguyên Cơ bản": [
        "basic resources", "mining", "coal mining", "thermal coal",
        "coking coal", "than nhiệt", "than luyện cốc", "quặng sắt",
        "iron ore", "đất hiếm", "rare earth", "bauxite", "alumina",
        "nhôm", "aluminum", "nickel", "lithium", "kẽm", "zinc",
        "thiếc", "tin", "chì", "lead", "đồng cathode", "copper",
        "apatit", "đá vôi", "limestone", "cát trắng", "silica sand",
        "mỏ đá", "đá ốp lát", "khai thác than", "giấy phép khai thác",
        "trữ lượng mỏ", "sản lượng khai thác"
    ],

    "Hàng cá nhân & Gia dụng": [
        "personal goods", "household goods", "consumer discretionary",
        "textile", "garment", "apparel", "footwear", "giày dép",
        "túi xách", "vali", "dệt", "nhuộm", "vải", "bông", "cotton",
        "polyester", "xơ sợi", "sợi cotton", "đơn hàng dệt may",
        "xuất khẩu dệt may", "xuất khẩu da giày", "gỗ nội thất",
        "furniture", "wood products", "home appliance", "đồ gia dụng",
        "thiết bị gia dụng", "điện gia dụng", "mỹ phẩm", "chăm sóc cá nhân",
        "personal care", "trang sức", "đồng hồ", "hàng xa xỉ", "luxury goods"
    ]
}



RAW_TICKER_DATA = """
"icb_name2","tickers"
"OTHER","DPD"
"Du lịch và Giải trí","ATS, ATS, BCV, BCV, BLN, BLN, BSG, BSG, BTV, BTV, CTC, DAH, DAH, DAR, DBH, DLD, DLD, DLT, DLT, DNT, DNT, DSD, DSD, DSN, DSN, DSP, DSP, DTI, DTI, DXL, DXL, EIN, EIN, HES, HES, HGT, HGT, HHG, HHG, HNT, HOT, HOT, HRT, HVN, HVN, KLF, MAS, MTC, NWT, NWT, ONW, ONW, PDC, PDC, PGT, PNG, PNG, RIC, RIC, SCS, SCS, SGH, SHX, SKG, SKG, SRT, STT, STT, TCT, TCT, TPS, TPS, TSD, TSD, TSJ, TSJ, TTT, VIR, VIR, VJC, VJC, VNG, VNG, VNS, VNS, VPL, VPL, VTD, VTD, VTG, VTG, VTM, VTM, VTR, VTR"
"Điện, nước & xăng dầu khí đốt","ASP, ASP, AVC, AVC, BDW, BDW, BGE, BGE, BGW, BGW, BHA, BHA, BLW, BMF, BMF, BNW, BNW, BSA, BSA, BTP, BTP, BTW, BWA, BWA, BWE, BWE, BWS, BWS, CCI, CCI, CHP, CHP, CHS, CHS, CLW, CLW, CMW, CMW, CNG, CNG, CTW, CTW, DBW, DDG, DDG, DKW, DMS, DMS, DNA, DNA, DNC, DNC, DNH, DNH, DNN, DNN, DNW, DNW, DRL, DRL, DRL, DTE, DTE, DTK, DTV, DVC, DVC, DWC, DWC, DWS, DWS, EAD, GAS, GAS, GCB, GCB, GDW, GE2, GEG, GEG, GEG, GHC, GLW, GLW, GSM, HDW, HDW, HFC, HFC, HGW, HIO, HIO, HJS, HNA, HNA, HND, HPD, HPD, HPW, HPW, HTC, HTW, HWS, ISH, ISH, KHP, KHP, KHW, KTW, KWA, LAW, LAW, LCW, LDW, LDW, LKW, LWS, MTG, MTG, NAW, NAW, NBP, NBP, NBT, NBT, NBW, ND2, ND2, NDW, NDW, NLS, NLS, NNT, NQB, NQB, NQN, NQN, NQT, NS2, NS2, NS3, NSL, NSL, NT2, NT2, NTH, NTW, NTW, NVP, NVP, PCG, PCG, PEG, PEG, PGC, PGC, PGD, PGD, PGS, PGV, PGV, PIC, PJC, PJS, PJS, PMG, PMG, PMW, PMW, PND, PND, POB, POB, POV, POV, POW, PPC, PPC, PPT, PPY, PSH, PSH, PTH, PTH, PTX, PVG, PWS, PWS, QNW, QNW, QPH, QPH, QTP, REE, REE, S4A, S4A, SBA, SBA, SBH, SBH, SEB, SFC, SFC, SHP, SHP, SII, SII, SJD, SJD, SMA, SMA, SP2, SP2, STW, STW, SVH, SVH, TAW, TBC, TBC, TBW, TDB, TDB, TDG, TDG, TDM, TDM, TDW, THN, THN, THW, THW, TLP, TLP, TMC, TMP, TMP, TNW, TNW, TOW, TOW, TQW, TQW, TTA, TTA, TTE, TTE, TVW, UIC, UIC, UPC, UPC, VAV, VAV, VCP, VCP, VCW, VLW, VLW, VMG, VMG, VPD, VPD, VPW, VPW, VSH, VSH, VWS, VWS, XMP, XMP"
"Dầu khí","BSR, BSR, OIL, OIL, PEQ, PEQ, PLX, PLX, PLX, POS, POS, PTV, PTV, PVB, PVC, PVD, PVD, PVE, PVE, PVS, TOS, TOS"
"Ngân hàng","ABB, ABB, ACB, ACB, BAB, BAB, BID, BID, BID, BVB, BVB, CTG, CTG, CTG, EIB, EIB, HDB, HDB, KLB, KLB, LPB, LPB, LPB, MBB, MBB, MSB, MSB, NAB, NAB, NVB, OCB, OCB, OCB, PGB, PGB, SGB, SGB, SHB, SHB, SSB, SSB, STB, STB, TCB, TCB, TPB, TPB, VAB, VAB, VBB, VBB, VCB, VCB, VIB, VIB, VPB, VPB"
"Thực phẩm và đồ uống","AAM, AAM, ABT, ABT, ACL, ACL, AGF, AGF, AGM, AGM, AIG, AIG, ANT, ANT, ANV, ANV, APF, APF, APT, APT, ASM, ASM, ASM, ATA, ATA, AUM, BAF, BAF, BBC, BBM, BBM, BCF, BCF, BHG, BHG, BHK, BHK, BHN, BHN, BHP, BHP, BIG, BIG, BKH, BLF, BLF, BLT, BLT, BMV, BMV, BNA, BNA, BQB, BQB, BSD, BSD, BSH, BSH, BSL, BSL, BSP, BSP, BSQ, BSQ, BTB, BTB, C22, C22, CAD, CAD, CAN, CAT, CAT, CCA, CCA, CFV, CFV, CLX, CLX, CMF, CMF, CMM, CMM, CMN, CMN, CMX, CMX, CNA, CNA, CPA, CPA, CTP, DAT, DAT, DBC, DBC, DMN, DMN, EPC, FCS, FCS, FGL, FGL, FMC, FMC, GCF, GCF, HAD, HAF, HAF, HAG, HAG, HAT, HBH, HBH, HDS, HHC, HKB, HKB, HKT, HLB, HLG, HNF, HNF, HNG, HNG, HNM, HNM, HNR, HNR, HPA, HSL, HSL, HSL, ICF, ICF, IDI, IDI, IDI, IDP, IDP, IFS, IFS, JOS, KDC, KDC, KHS, KHS, KSH, KTC, KTC, KTS, LAF, LAF, LSS, LSS, LSS, LTG, LYF, MCF, MCH, MCH, MCM, MCM, MLS, MLS, MML, MPC, MPC, MSN, MSN, NAF, NAF, NCG, NCG, NCS, NCS, NGC, NSC, NSC, NSS, NSS, OCH, PAN, PAN, PCF, PCF, PRO, PRO, PSL, PSL, QHW, QHW, QNS, QNS, SAB, SAB, SAF, SB1, SB1, SBB, SBB, SBL, SBL, SBT, SBT, SCD, SCD, SCV, SEA, SEA, SGC, SJ1, SKH, SKH, SKN, SKN, SKV, SKV, SLS, SMB, SMB, SNC, SNC, SPD, SPH, SPH, SPV, SPV, SSC, SSN, SSN, STD, TAN, TAR, TCJ, TCO, TCO, TFC, THB, THP, THP, TS4, TS4, TT6, TT6, UXC, UXC, VCF, VCF, VDL, VHC, VHC, VHF, VHF, VKD, VLC, VLC, VNH, VNH, VNM, VNM, VOC, VSF, VSF, VSN, VSN, VTL, WSB, WSB"
"Dịch vụ tài chính","AAS, AAS, ABW, ABW, AGR, AGR, APG, APG, APS, ART, ART, BCG, BCG, BMS, BMS, BSI, BSI, BVS, CSI, CSI, CTS, CTS, DCV, DSC, DSC, DSE, DSE, EVF, EVF, EVS, F88, F88, FTS, FTS, HAC, HAC, HBS, HCM, HCM, HVA, HVA, IBC, IVS, MBS, OGC, OGC, ORS, ORS, PHS, PHS, PSI, SBS, SBS, SHS, SSI, SSI, TCI, TCI, TCX, TCX, TIN, TIN, TVB, TVB, TVC, TVC, TVS, TVS, VCI, VCI, VCK, VCK, VDS, VDS, VFS, VIG, VIX, VIX, VIX, VND, VND, VPX, VPX, VUA, WSS"
"Tài nguyên Cơ bản","AAH, AAH, ACG, ACG, ACM, ACM, AMC, ATG, ATG, BCA, BCA, BCB, BCB, BKC, BMC, BMC, BMJ, BMJ, BVG, BVG, CAP, CBI, CBI, CLM, CLM, CST, D17, DFC, DFC, DHC, DHC, DHM, DHM, DTL, DTL, FRC, FRC, FRM, FRM, GDA, GDA, GLC, GLC, GTA, GTA, GVT, GVT, HAP, HAP, HAP, HGM, HHP, HHP, HLC, HMC, HMG, HMG, HPG, HPG, HPM, HPM, HSG, HSG, HSV, HSV, ITQ, KCB, KHB, KHD, KKC, KLM, KMT, KSV, KTL, KTL, KVC, KVC, LCM, LCM, LMC, MC3, MDC, MDC, MDF, MDF, MEL, MGC, MGC, MHL, MHL, MIC, MIM, MSR, MSR, MTA, MTA, MVB, MZG, MZG, NBC, NHV, NKG, NKG, NSH, PAS, PAS, PIS, PIS, POM, POM, PTB, PTB, SAV, SAV, SCA, SHA, SHA, SHI, SHI, SHN, SHN, SJF, SJF, SMC, SMC, SPI, SPI, SQC, SSM, TC6, TD6, TDN, TDS, TDS, THT, THT, TIS, TIS, TKG, TKU, TLH, TLH, TMB, TMG, TMG, TMW, TMW, TNA, TNA, TNB, TNB, TNI, TNI, TNI, TNS, TNS, TQN, TQN, TTF, TTF, TTH, TTH, TTS, TTS, TVD, TVN, TVN, VBG, VBG, VCA, VCA, VDB, VDB, VDT, VDT, VGL, VGL, VGS, VID, VID, VIF, VIM, VIM, VIS, VLS, VPG, VPG, YBM, YBM"
"Bảo hiểm","ABI, ABI, AIC, AIC, BHI, BHI, BIC, BIC, BLI, BMI, BMI, BMI, BVH, BVH, MIG, MIG, PGI, PGI, PRE, PTI, PVI, VNR, VNR"
"Ô tô và phụ tùng","CSM, CSM, CTF, CTF, DAS, DAS, DRC, DRC, FTI, FTI, GGG, GGG, GMA, HAX, HAX, HHS, HHS, HTL, HTL, HUT, PTM, SRC, SRC, SVC, SVC, TMT, TMT, VKC, VKC, VMA, VMA, VVS, VVS"
"Bán lẻ","AFX, AFX, AGX, AGX, AST, AST, AST, BTT, BTT, BTT, CEN, CEN, CGL, CMV, CMV, COM, COM, CPH, CPH, DGW, DGW, DKC, DKC, FHN, FHN, FRT, FRT, HFX, HFX, HTM, HTM, HTT, HTT, KGM, LBC, MWG, MWG, PET, PET, PET, PIT, PIT, PSD, SAS, SAS, SBV, SBV, SVT, SVT, TH1, TH1, THS, TOP, TOP"
"Hàng & Dịch vụ Công nghiệp","ABR, ACV, ACV, AGE, ALC, ALC, AME, AMS, AMS, APH, APH, APL, APL, ARM, ASG, ASG, BAL, BAL, BBH, BBH, BBS, BMD, BMD, BPC, BRS, BRS, BTG, BTG, BTH, BTH, BTU, BTU, BXH, CAG, CAR, CAR, CAV, CCP, CCP, CCR, CCT, CDH, CDN, CE1, CFM, CFM, CIA, CJC, CKA, CKA, CKD, CKD, CLL, CMC, CMK, CMK, CMP, CMP, CPI, CPI, CQN, CQN, CTB, CTT, CTT, CVP, DDH, DDH, DDM, DDM, DHP, DL1, DL1, DNE, DNE, DNL, DNL, DOP, DOP, DPC, DPC, DS3, DS3, DSV, DT4, DTB, DUS, DUS, DVP, DVP, DXP, DZM, DZM, EGL, EGL, EMC, EME, EME, EMG, EMG, EMS, EMS, FBC, FBC, FSO, FSO, FT1, FT1, GEE, GEE, GEX, GEX, GIC, GMD, GMD, GSP, GSP, HAH, HAH, HBD, HBD, HCT, HEC, HEC, HEM, HEP, HEP, HHN, HHN, HHR, HKP, HLO, HLO, HLR, HLS, HMH, HMH, HNB, HNB, HPB, HPB, HSA, HSA, HTE, HTE, HTR, HTV, HTV, HTV, ILB, ILB, ILC, ILC, ILS, ILS, IME, IME, INN, IPA, ISG, ISG, IST, IST, ITS, ITS, KIP, L10, L10, L35, L35, L44, L61, L61, L62, L62, L63, L63, LLM, LM7, LM7, LMI, LO5, LO5, LPT, MAC, MBN, MBN, MCP, MCP, MDA, MHC, MHC, MIE, MLC, MLC, MND, MND, MPY, MPY, MQB, MQN, MTB, MTB, MTH, MTH, MTL, MTL, MTS, MTS, MTV, MTV, MTX, MVN, MVN, NAG, NAP, NAS, NAS, NAU, NAU, NCT, NCT, NEM, NO1, NO1, NOS, NOS, NUE, NUE, PAC, PAC, PAP, PAP, PBP, PBT, PBT, PCT, PDN, PDN, PDN, PDV, PDV, PEC, PHN, PHP, PHP, PJT, PJT, PLO, PLO, PMP, PMS, PNP, PNP, PPE, PPS, PQN, PQN, PRC, PSC, PSN, PSN, PSP, PTS, PTS, PTT, PVM, PVM, PVP, PVP, PVT, PVT, QHD, QLT, QNP, QNP, QNU, QNU, QSP, QSP, RAT, RAT, SAC, SAC, SAL, SAL, SBG, SBG, SCO, SCO, SCY, SDA, SDC, SDG, SDG, SDK, SDK, SDV, SDV, SFI, SFI, SFN, SGN, SGN, SGP, SGS, SHC, SHC, SPA, SRF, SRF, SRF, SSG, SSG, SSU, STG, STG, STP, STS, STS, SVG, SVG, SVI, SVI, SWC, SWC, SZE, SZE, TAB, TAB, TAP, TB8, TB8, TBD, TBH, TCL, TCL, TCW, TCW, TDP, TDP, TGP, TGP, THI, THU, THU, TIE, TIE, TJC, TKA, TKA, TMS, TMS, TNP, TNP, TOT, TPP, TR1, TR1, TRS, TRS, TRV, TRV, TSB, TSG, TSG, TTP, TUG, TV1, TV1, TV2, TV2, TV4, TVM, TYA, TYA, TYA, UCT, UCT, UDL, UDL, UEM, USD, USD, UTT, UTT, VBC, VBH, VBH, VCM, VCT, VCT, VEA, VEA, VFC, VFC, VFR, VFR, VGP, VGR, VGR, VHG, VHG, VIN, VIN, VIP, VIP, VLG, VLG, VLP, VLP, VMS, VMT, VMT, VNA, VNA, VNC, VNF, VNF, VNL, VNL, VNT, VOS, VOS, VPA, VPA, VQC, VQC, VSA, VSC, VSC, VSE, VSE, VSG, VSG, VSM, VST, VST, VTB, VTB, VTH, VTK, VTK, VTO, VTO, VTP, VTP, VTX, VTX, VXT, VXT, WCS, WTC, WTC"
"Bất động sản","AAV, AAV, AGG, AGG, AMD, API, BCM, BCM, BII, BSC, BVL, BVL, C21, C21, CCL, CCL, CEO, CK8, CK8, CKG, CKG, CNT, CNT, CRE, CRE, CRV, CRV, CRV, D11, D2D, D2D, DCH, DCH, DIG, DIG, DRH, DRH, DTA, DTA, DTD, DXG, DXG, DXS, DXS, DXS, EFI, EFI, FCC, FCC, FDC, FDC, FIR, FIR, FLC, HAR, HAR, HD2, HD2, HD6, HD6, HD8, HD8, HDC, HDC, HDC, HDG, HDG, HDG, HLD, HLD, HPI, HPI, HPX, HPX, HQC, HQC, HRB, HRB, HTN, HTN, HU6, HU6, IDC, IDJ, IDV, IJC, IJC, ITA, ITA, ITC, ITC, ITC, KBC, KBC, KDH, KDH, KHA, KHG, KHG, KOS, KOS, KSF, LDG, LDG, LEC, LEC, LGL, LGL, LHG, LHG, LMH, LSG, LSG, MA1, MA1, MBT, MBT, MGR, MGR, MH3, MH3, NBB, NBB, NDN, NLG, NLG, NRC, NTC, NTC, NTL, NTL, NVL, NVL, NVT, NVT, PDR, PDR, PIV, PIV, PPI, PPI, PRT, PRT, PTL, PTL, PV2, PV2, PVL, PVL, PVR, PWA, PWA, PXA, PXA, PXC, PXL, PXL, QCG, QCG, RCL, RGG, RGG, SCR, SCR, SGR, SGR, SID, SID, SIP, SIP, SJS, SJS, SLD, SLD, SSH, SSH, SZB, SZC, SZC, SZG, SZG, SZL, SZL, TAG, TAL, TAL, TBR, TCH, TCH, TDC, TDC, TDH, TDH, TDH, TEG, TEG, THD, TID, TID, TIG, TIP, TIP, TIP, TIX, TN1, TN1, TN1, TV6, TV6, V11, V21, V21, VC3, VCR, VCR, VEF, VEF, VES, VES, VGV, VGV, VHD, VHD, VHM, VHM, VIC, VIC, VNI, VNI, VPH, VPH, VPI, VPI, VRC, VRC, VRE, VRE, VRG, VRG, XDH, XDH"
"Viễn thông","ABC, ABC, FOX, FOX, MFS, MFS, PAI, PAI, PIA, PTP, TTN, TTN, VGI, VGI"
"Y tế","AGP, AMP, AMP, AMV, APC, APC, BCP, BCP, BIO, BIO, CDP, CDP, CNC, CNC, CVN, CVN, DAN, DAN, DBD, DBD, DBM, DBM, DBT, DBT, DCL, DCL, DDN, DDN, DHD, DHD, DHG, DHG, DHN, DHN, DHT, DMC, DMC, DNM, DNM, DP1, DP1, DP2, DP2, DP3, DP3, DPH, DPH, DPP, DPP, DTG, DTH, DTH, DTP, DTP, DVM, DVN, DVN, FIT, FIT, GPC, GPC, HDP, HDP, IMP, IMP, JVC, JVC, LDP, MED, MEF, MEF, MKP, MKP, MKV, MRF, MRF, MTP, MTP, NDC, NDC, NDP, NTF, NTF, OPC, OPC, OPC, PBC, PBC, PMC, PPP, SPM, SPM, SRA, TNH, TNH, TRA, TRA, TTD, TTD, TVP, TW3, UPH, UPH, VDP, VDP, VHE, VMD, VMD, YTC, YTC"
"Hóa chất","AAA, AAA, ABS, APP, APP, AVG, AVG, BFC, BFC, BQP, BQP, BRC, BRC, BRR, BRR, BT1, BT1, CPC, CSV, CSV, DAG, DAG, DCM, DCM, DDV, DDV, DGC, DGC, DHB, DHB, DOC, DOC, DPM, DPM, DPR, DPR, DRG, DRG, DRI, DRI, DTT, DTT, ECO, ECO, GER, GER, GVR, GVR, HAI, HCD, HCD, HCD, HII, HII, HMD, HMD, HNP, HNP, HPH, HPH, HRC, HRC, HSI, HSI, HVT, IRC, IRC, LAS, LNC, NFC, NFC, NHH, NHH, NNG, NSG, NSG, PAT, PAT, PCE, PCE, PCH, PCN, PGN, PHR, PHR, PLC, PLP, PLP, PMB, PMB, PSE, PSW, PVO, PVO, QBS, QBS, RBC, RBC, RDP, RDP, RTB, RTB, SBR, SBR, SEP, SEP, SFG, SFG, SPC, SPC, TNC, TPC, TPC, TRC, TRC, TSC, VAF, VAF, VET, VET, VFG, VFG, VNP, VNP, VNY, VNY, VPS, VPS, VTQ, VTQ, VTZ, VTZ, VXP, VXP"
"Công nghệ Thông tin","CKV, CMG, CMG, CMT, CMT, ELC, ELC, FPT, FPT, HIG, HPT, HPT, ICT, ICT, ITD, ITD, ITD, KST, LTC, ONE, PMJ, PMJ, PMT, PMT, POT, SAM, SAM, SBD, SBD, SGT, SGT, SMT, SRB, SRB, ST8, ST8, TST, TST, UNI, VEC, VEC, VIE, VIE, VLA, VLA, VTC, VTE, VTE"
"Xây dựng và Vật liệu","ACC, ACC, ACE, ACS, ADP, ADP, ALV, ALV, ATB, BAX, BCC, BCE, BCE, BCO, BCR, BCR, BDT, BDT, BHC, BHC, BHT, BMN, BMP, BMP, BOT, BOT, BT6, BT6, BTD, BTD, BTN, BTN, BTS, C32, C32, C32, C47, C47, C47, C4G, C4G, C69, C92, C92, CC1, CC1, CC4, CCC, CCC, CCC, CCM, CCM, CCV, CCV, CDC, CDC, CDG, CDG, CDO, CDO, CDR, CDR, CEE, CEG, CGV, CH5, CH5, CHC, CHC, CI5, CI5, CID, CID, CIG, CIG, CII, CII, CIP, CIP, CLH, CMD, CMD, CMI, CMI, CMS, CNN, CNN, CQT, CQT, CRC, CRC, CSC, CT3, CT3, CT6, CT6, CTD, CTD, CTD, CTI, CTI, CTR, CTR, CTX, CTX, CVT, CVT, CX8, CYC, CYC, DAC, DAC, DC1, DC1, DC2, DC4, DC4, DCF, DCF, DCR, DCT, DCT, DDB, DDB, DFF, DFF, DGT, DGT, DHA, DHA, DIC, DIC, DID, DID, DIH, DKG, DKG, DLG, DLG, DLR, DLR, DND, DND, DNP, DPG, DPG, DSG, DSG, DSH, DSH, DTC, DTC, DVG, DVG, DVW, DVW, DXV, DXV, E12, E12, E29, E29, EIC, EIC, EVG, EVG, FCM, FCM, FCN, FCN, FIC, FIC, FID, FID, G36, G36, GAB, GEL, GH3, GH3, GKM, GMH, GMH, GMX, GND, GND, GTH, GTS, GTS, H11, H11, HAM, HAM, HAN, HAN, HAS, HAS, HBC, HBC, HC1, HC1, HC3, HCC, HCI, HCI, HDA, HEJ, HEJ, HFB, HFB, HHV, HHV, HID, HID, HLE, HLY, HMR, HMR, HMS, HOM, HPP, HPP, HSP, HSP, HT1, HT1, HTI, HTI, HU1, HU1, HU3, HU3, HU4, HU4, HUB, HUB, HVH, HVH, HVX, HVX, ICC, ICC, ICG, ICI, ICI, ICN, ICN, ILA, ILA, INC, ING, KCE, KDM, KDM, KPF, KPF, KSB, KSB, KSQ, KSQ, KTT, KTT, L12, L12, L14, L14, L18, L40, L43, L45, L45, LAI, LAI, LBM, LBM, LBM, LCC, LCC, LCD, LCG, LCG, LCS, LG9, LGC, LGC, LHC, LIC, LIC, LIG, LM3, LM8, LM8, LQN, LUT, MBG, MCC, MCG, MCG, MCI, MCO, MDG, MDG, MEC, MEC, MES, MES, MST, MVC, MVC, NAC, NAC, NAV, NAV, NDX, NDX, NED, NHA, NHA, NHC, NNC, NNC, NTP, NXT, NXT, PC1, PC1, PCC, PCC, PCM, PCM, PDB, PEN, PFL, PFL, PHC, PHC, PHC, PHH, PHH, PID, PID, PLA, PLE, PLE, PNT, PNT, PSB, PSB, PTC, PTC, PTD, PTD, PTE, PTO, PVA, PVH, PVH, PVV, PVV, PVX, PVX, PVY, PVY, PXI, PXI, PXM, PXM, PXS, PXS, PXT, PXT, QCC, QCC, QLD, QNC, QNC, QNT, QNT, QTC, RCC, RCC, RCD, RCD, ROS, RYG, RYG, S12, S12, S55, S55, S72, S72, S74, S74, S99, SBM, SBM, SC5, SC5, SCC, SCC, SCG, SCI, SCJ, SCJ, SCL, SCL, SD2, SD2, SD3, SD3, SD4, SD4, SD5, SD6, SD6, SD7, SD7, SD9, SDD, SDD, SDJ, SDN, SDP, SDP, SDT, SDT, SDU, SDY, SDY, SHG, SHG, SIC, SIG, SIV, SJC, SJC, SJE, SJG, SJG, SJM, SNZ, SNZ, SVN, TA3, TA6, TA6, TA9, TBX, TCD, TCD, TCK, TCR, TCR, TDF, TDF, TDI, TEC, TED, TED, TEL, TGG, TGG, THG, THG, TKC, TKC, TL4, TL4, TLD, TLD, TLT, TLT, TMX, TNT, TNT, TRT, TRT, TS3, TS3, TSA, TSA, TTB, TTB, TTC, TTL, TTZ, TTZ, TV3, TVA, TVG, TVG, TVH, TXM, UDC, UDC, UDJ, UDJ, UMC, USC, V12, V12, VC1, VC2, VC5, VC5, VC6, VC7, VC9, VCC, VCE, VCE, VCG, VCG, VCS, VCX, VCX, VE1, VE2, VE2, VE3, VE3, VE4, VE8, VE8, VE9, VE9, VGC, VGC, VHH, VHH, VHL, VIH, VIH, VIT, VIW, VIW, VLB, VLB, VMC, VMK, VMK, VNE, VNE, VPC, VPC, VSI, VSI, VTA, VTA, VTS, VTS, VTV, VVN, VW3, VW3, VXB, VXB, X77, X77, XDC, XLV, XLV, XMC, XMC, XMD, XMD, YBC, YBC"
"Truyền thông","ADC, ADC, ADG, ALT, BDB, BED, BST, CAB, DAD, DAE, DST, EBS, ECI, EID, EPH, FHS, FHS, FOC, FOC, HEV, HTP, IBD, IBD, IBN, IHK, IHK, IN4, IN4, NBE, NBE, ODE, ODE, PNC, PNC, QST, QST, RGC, SAP, SED, SGD, SMN, STC, STH, STH, TPH, VNB, VNB, VNX, VNX, VNZ, VNZ, VPR, VPR, YEG, YEG"
"Hàng cá nhân & Gia dụng","A32, A32, AAT, AAT, ADS, ADS, ADS, AG1, AG1, BBT, BBT, BDG, BDG, BEL, BEL, BKG, BMG, BMG, BVN, BVN, CET, CLC, CLC, DCG, DCG, DCS, DCS, DM7, DM7, DQC, DQC, EVE, EVE, FTM, FTM, G20, G20, GDT, GDT, GIL, GLT, GMC, GMC, GTD, GTD, HCB, HDM, HDM, HJC, HJC, HLT, HNI, HNI, HSM, HSM, HTG, HTG, HUG, HUG, KMR, KMR, KSD, LBE, LGM, LIX, M10, M10, MEG, MGG, MGG, MNB, MNB, MPT, MSH, MSH, NDT, NDT, NET, NET, NHT, NHT, NJC, NST, NTT, NTT, PNJ, PNJ, PPH, PPH, PTG, RAL, RAL, SGI, SGI, SHE, SHE, SPB, SPB, SSF, SSF, STK, STK, SVD, SVD, TCM, TCM, TDT, TET, THM, THM, TLG, TLG, TLI, TNG, TNV, TNV, TTG, TTG, TVT, TVT, VDG, VDG, VDM, VDN, VDN, VGG, VGG, VGT, VGT, VTI, VTI, VTJ, X20, X26, X26, XHC, XHC, XPH, XPH"
"""

def prepare_sector_dictionary() -> Dict[str, re.Pattern]:
    reader = csv.reader(io.StringIO(RAW_TICKER_DATA.strip()))
    next(reader, None)

    for row in reader:
        if len(row) != 2: continue
        sector_name = row[0].strip()
        raw_tickers = [t.strip().lower() for t in row[1].split(",")]
        clean_tickers = list(set(raw_tickers))

        if sector_name not in BASE_SECTOR_DICT:
            BASE_SECTOR_DICT[sector_name] = []
        BASE_SECTOR_DICT[sector_name].extend(clean_tickers)

    compiled_patterns = {}
    for sector, keywords in BASE_SECTOR_DICT.items():
        sorted_keywords = sorted(list(set(keywords)), key=len, reverse=True)
        escaped_keywords = [re.escape(k) for k in sorted_keywords]
        pattern_str = r'\b(' + '|'.join(escaped_keywords) + r')\b'
        compiled_patterns[sector] = re.compile(pattern_str, re.IGNORECASE)
    return compiled_patterns

COMPILED_SECTOR_PATTERNS = prepare_sector_dictionary()

def classify_sector_fast(text: str) -> str:
    if not isinstance(text, str) or not text.strip():
        return "OTHER"
    
    text_lower = text.lower()
    sector_scores = {sector: 0 for sector in COMPILED_SECTOR_PATTERNS.keys()}
    for sector, compiled_regex in COMPILED_SECTOR_PATTERNS.items():
        matches = compiled_regex.findall(text_lower)
        sector_scores[sector] += len(matches)

    max_score = max(sector_scores.values())
    if max_score == 0:
        return "OTHER"
    return max(sector_scores, key=sector_scores.get)

# ==========================================
# 2. SENTIMENT PROCESSOR
# ==========================================
# Lazy load model to avoid memory overhead unless necessary
_sentiment_pipeline = None

def load_sentiment_model():
    global _sentiment_pipeline
    if _sentiment_pipeline is None:
        logger.info("Loading PhoBERT sentiment model...")
        import torch
        from transformers import pipeline
        device = 0 if torch.cuda.is_available() else -1
        model_name = "wonrax/phobert-base-vietnamese-sentiment"
        _sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model=model_name,
            tokenizer=model_name,
            device=device
        )
        logger.info("PhoBERT sentiment model loaded successfully.")
    return _sentiment_pipeline

def compute_sentiment_score(label: str, score: float) -> float:
    """
    Convert model output to a float in range [-100.0, 100.0]
    POS -> +score*100
    NEG -> -score*100
    NEU -> 0.0
    """
    percent_score = score * 100
    if label == "POS":
        return float(percent_score)
    elif label == "NEG":
        return -float(percent_score)
    return 0.0

def fill_sentiment(db_url: str, schema: str, table: str = "news", batch_size: int = 32) -> str:
    logger.info("START FILL SENTIMENT")
    try:
        with closing(get_postgres_connection(db_url)) as conn:
            # ĐƯA LÊN ĐÂY: Thiết lập ngay khi vừa kết nối
            conn.autocommit = False 
            
            # 1. Fetch rows with NULL sentiment
            fetch_sql = f"""
                SELECT source, title, link
                FROM {schema}.{table}
                WHERE sentiment IS NULL AND title IS NOT NULL
                LIMIT 5000;
            """
            with conn.cursor() as cur:
                cur.execute(fetch_sql)
                rows = cur.fetchall()
            
            if not rows:
                logger.info("No rows require sentiment filling.")
                return "Success: 0 rows"
                
            logger.info(f"Found {len(rows)} rows to fill sentiment.")
            
            # 2. Setup model and process in batches
            model = load_sentiment_model()
            updates = []
            
            # Process in chunks
            for i in range(0, len(rows), batch_size):
                batch_rows = rows[i:i+batch_size]
                titles = [r[1] for r in batch_rows]
                links = [r[2] for r in batch_rows]
                
                # Truncation and max_length limits memory issues
                results = model(titles, batch_size=batch_size, truncation=True, max_length=256)
                
                for link, result in zip(links, results):
                    score = compute_sentiment_score(result['label'], result['score'])
                    updates.append((score, link))
                    
            # 3. Update DB
            update_sql = f"""
                UPDATE {schema}.{table}
                SET sentiment = data.sentiment
                FROM (VALUES %s) AS data(sentiment, link)
                WHERE {schema}.{table}.link = data.link;
            """
            
            # XÓA DÒNG conn.autocommit = False Ở ĐÂY ĐI
            with conn.cursor() as cur:
                execute_values(cur, update_sql, updates)
            conn.commit()
            
            logger.info(f"Successfully updated {len(updates)} rows with sentiment scores.")
            return f"Success: {len(updates)} rows"
            
    except Exception as e:
        logger.error(f"Error filling sentiment: {e}")
        logger.error(traceback.format_exc())
        raise

# ==========================================
# 3. ICB NAME PROCESSOR
# ==========================================
def fill_icb_name(db_url: str, schema: str, table: str = "news") -> str:
    logger.info("START FILL ICB NAME")
    try:
        with closing(get_postgres_connection(db_url)) as conn:
            # SỬA Ở ĐÂY: Chuyển cấu hình autocommit lên ngay sau khi mở kết nối, 
            # TRƯỚC khi có bất kỳ câu lệnh SQL (kể cả SELECT) nào được chạy.
            conn.autocommit = False
            
            # 1. Fetch rows with NULL icb_name
            fetch_sql = f"""
                SELECT source, title, link
                FROM {schema}.{table}
                WHERE icb_name IS NULL AND title IS NOT NULL
                LIMIT 5000;
            """
            with conn.cursor() as cur:
                cur.execute(fetch_sql)
                rows = cur.fetchall()
                
            if not rows:
                logger.info("No rows require ICB name filling.")
                return "Success: 0 rows"
                
            logger.info(f"Found {len(rows)} rows to fill ICB name.")
            
            # 2. Process ICB classification
            updates = []
            for row in rows:
                title = row[1]
                link = row[2]
                icb_name = classify_sector_fast(title)
                updates.append((icb_name, link))
                
            # 3. Update DB
            update_sql = f"""
                UPDATE {schema}.{table}
                SET icb_name = data.icb_name
                FROM (VALUES %s) AS data(icb_name, link)
                WHERE {schema}.{table}.link = data.link;
            """
            
            # Đã xóa dòng conn.autocommit = False bị lỗi ở đây
            with conn.cursor() as cur:
                execute_values(cur, update_sql, updates)
            conn.commit()
            
            logger.info(f"Successfully updated {len(updates)} rows with ICB names.")
            return f"Success: {len(updates)} rows"
            
    except Exception as e:
        logger.error(f"Error filling ICB name: {e}")
        logger.error(traceback.format_exc())
        raise