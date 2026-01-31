create table history_price
(
    ticker       varchar(10) not null,
    trading_date text        not null,
    open         numeric(15, 2),
    high         numeric(15, 2),
    low          numeric(15, 2),
    close        numeric(15, 2),
    volume       bigint,
    import_time  timestamp default CURRENT_TIMESTAMP,
    constraint pk_history_price
        primary key (ticker, trading_date)
);

alter table history_price
    owner to admin;

create table market_index
(
    ticker       varchar(10),
    trading_date text,
    open         numeric(15, 2),
    high         numeric(15, 2),
    low          numeric(15, 2),
    close        numeric(15, 2),
    volume       bigint,
    import_time  timestamp default CURRENT_TIMESTAMP
);

alter table market_index
    owner to admin;

create table owner
(
    ticker      varchar(10),
    name        varchar(255),
    position    varchar(255),
    percent     text,
    type        varchar(50),
    import_time timestamp default CURRENT_TIMESTAMP
);

alter table owner
    owner to admin;

create table company_overview
(
    ticker           varchar(10) not null
        constraint pk_company_overview
            primary key
        constraint ticker_unique
            unique,
    overview         text,
    icb_name1        text,
    icb_name2        text,
    icb_name3        text,
    import_time      timestamp default CURRENT_TIMESTAMP,
    exchange         text,
    type_info        text,
    organ_short_name text,
    organ_name       text,
    product_group    text
);

alter table company_overview
    owner to admin;

create table news
(
    news_id          varchar(50) not null
        primary key,
    ticker           varchar(10),
    title            text        not null,
    sub_title        text,
    short_content    text,
    full_content     text,
    image_url        text,
    source_link      text,
    source_name      varchar(255),
    lang_code        varchar(5) default 'vi'::character varying,
    public_date      timestamp,
    created_at       timestamp,
    fetched_at       timestamp  default CURRENT_TIMESTAMP,
    close_price      numeric(15, 2),
    ref_price        numeric(15, 2),
    floor_price      numeric(15, 2),
    ceiling_price    numeric(15, 2),
    price_change_pct numeric(10, 4),
    import_time      timestamp  default CURRENT_TIMESTAMP
);

alter table news
    owner to admin;

create table bctc
(
    ticker      varchar(10),
    quarter     varchar(10),
    year        integer,
    ind_name    text,
    ind_code    varchar(50),
    value       numeric(25, 4),
    import_time timestamp default CURRENT_TIMESTAMP,
    report_name varchar(255),
    report_code varchar(100)
);

alter table bctc
    owner to admin;

create table indicator_mapping_4bctc
(
    raw_ind_name text,
    std_ind_name text
);

alter table indicator_mapping_4bctc
    owner to admin;

create table macro_economy
(
    date       date,
    open       real,
    high       real,
    low        real,
    close      real,
    volume     bigint,
    asset_type varchar(20)
);

alter table macro_economy
    owner to admin;

create table financial_ratio
(
    id                           serial
        primary key,
    cp                           varchar(255),
    nam                          integer,
    ky                           integer,
    tsc_von_csh                  double precision,
    von_csh_von_ieu_le           double precision,
    bien_ebit                    double precision,
    bien_loi_nhuan_gop           double precision,
    bien_loi_nhuan_rong          double precision,
    ebit_ty_ong                  double precision,
    on_bay_tai_chinh             double precision,
    nan_1                        varchar(255),
    nan_2                        varchar(255),
    vay_nh_dh_vcsh               double precision,
    no_vcsh                      double precision,
    vong_quay_tai_san            double precision,
    vong_quay_tsc                double precision,
    so_ngay_thu_tien_binh_quan   double precision,
    so_ngay_ton_kho_binh_quan    double precision,
    so_ngay_thanh_toan_binh_quan double precision,
    chu_ky_tien                  double precision,
    vong_quay_hang_ton_kho       double precision,
    roe                          double precision,
    roic                         double precision,
    roa                          double precision,
    ebitda_ty_ong                double precision,
    chi_so_thanh_toan_hien_thoi  double precision,
    chi_so_thanh_toan_tien_mat   double precision,
    chi_so_thanh_toan_nhanh      double precision,
    kha_nang_chi_tra_lai_vay     double precision,
    von_hoa_ty_ong               double precision,
    so_cp_luu_hanh_trieu_cp      double precision,
    p_e                          double precision,
    p_b                          double precision,
    p_s                          double precision,
    p_cash_flow                  double precision,
    eps_vnd                      double precision,
    bvps_vnd                     double precision,
    ev_ebitda                    double precision,
    ty_suat_co_tuc               double precision
);

alter table financial_ratio
    owner to admin;

create table realtime_quotes
(
    symbol           varchar(20) not null,
    ts               timestamp   not null,
    last_price       numeric(18, 4),
    avg_price        numeric(18, 4),
    last_volume      bigint,
    total_volume     bigint,
    total_value      numeric(20, 2),
    foreign_buy_qty  bigint,
    foreign_sell_qty bigint,
    foreign_buy_val  numeric(20, 2),
    foreign_sell_val numeric(20, 2),
    bid1_price       numeric(18, 4),
    bid1_qty         bigint,
    bid2_price       numeric(18, 4),
    bid2_qty         bigint,
    bid3_price       numeric(18, 4),
    bid3_qty         bigint,
    ask1_price       numeric(18, 4),
    ask1_qty         bigint,
    ask2_price       numeric(18, 4),
    ask2_qty         bigint,
    ask3_price       numeric(18, 4),
    ask3_qty         bigint,
    ref_price        numeric(18, 4),
    ceil_price       numeric(18, 4),
    floor_price      numeric(18, 4),
    change_percent   numeric(10, 4),
    change_value     numeric(18, 4),
    high_price       numeric(18, 4),
    low_price        numeric(18, 4),
    constraint pk_realtime_quotes
        primary key (symbol, ts)
);

alter table realtime_quotes
    owner to admin;

