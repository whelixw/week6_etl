from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
import pandas as pd
from sqlalchemy import text, types, create_engine

#load user and password from file
with open("db_credentials.txt", "r") as f:
    lines = f.readlines()
    db_user = lines[0].strip()
    db_password = lines[1].strip()

# -------------------- CONNECTION/CONSTANTS --------------------

#use the loaded user and password to connect to the mysql database
engine = create_engine(
    "mysql+pymysql://"+db_user+":"+db_password+"@127.0.0.1:3307/mydb",
    pool_pre_ping=True,
)


VARCHAR_LEN = 191  # safer for utf8mb4 indexed columns in MySQL


# -------------------- UTILITIES --------------------

def _q(name: str) -> str: #quote string for sql
    return f"`{name}`"


def _cols(cols: Sequence[str]) -> str: #
    return ", ".join(_q(c) for c in cols)


def _constraint_name(prefix: str, table: str, cols: Sequence[str]) -> str:
    base = f"{prefix}_{table}_{'_'.join(cols)}"
    return base[:60]


def normalize_strings(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    df = df.copy()
    for c in columns:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()
    return df


def load_to_sql(
    df: pd.DataFrame,
    table_name: str,
    primary_key: Optional[Union[str, Sequence[str]]] = None,
    foreign_keys: Optional[List[Dict[str, Any]]] = None,
    unique: Optional[List[Sequence[str]]] = None,
    dtype: Optional[Dict[str, Any]] = None,
) -> None:
    if foreign_keys is None:
        foreign_keys = []
    if unique is None:
        unique = []

    df = df.drop_duplicates()
    df = df.convert_dtypes().infer_objects()

    with engine.begin() as conn:
        df.to_sql(
            table_name,
            con=conn,
            if_exists="replace",
            index=False,
            dtype=dtype,
            method=None,
        )

        for cols in unique:
            cols = list(cols)
            uc_name = _constraint_name("uq", table_name, cols)
            conn.execute(
                text(
                    f"ALTER TABLE {_q(table_name)} "
                    f"ADD CONSTRAINT {_q(uc_name)} UNIQUE ({_cols(cols)})"
                )
            )

        if primary_key:
            pk_cols = (
                [primary_key] if isinstance(primary_key, str) else list(primary_key)
            )
            conn.execute(
                text(
                    f"ALTER TABLE {_q(table_name)} "
                    f"ADD PRIMARY KEY ({_cols(pk_cols)})"
                )
            )

        for fk in foreign_keys:
            cols = list(fk["columns"])
            ref_table = fk["ref_table"]
            ref_cols = list(fk["ref_columns"])
            on_delete = fk.get("on_delete")
            on_update = fk.get("on_update")

            fk_name = _constraint_name("fk", table_name, cols)

            sql = (
                f"ALTER TABLE {_q(table_name)} "
                f"ADD CONSTRAINT {_q(fk_name)} "
                f"FOREIGN KEY ({_cols(cols)}) "
                f"REFERENCES {_q(ref_table)} ({_cols(ref_cols)})"
            )
            if on_delete:
                sql += f" ON DELETE {on_delete}"
            if on_update:
                sql += f" ON UPDATE {on_update}"

            conn.execute(text(sql))

        result = conn.execute(text(f"SELECT COUNT(*) FROM {_q(table_name)}"))
        print(f"{table_name} count:", result.scalar())


# -------------------- EXTRACT --------------------

def extract() -> Dict[str, pd.DataFrame]:
    # Base CSVs
    brands = pd.read_csv("data_setup/Data CSV/brands.csv")
    categories = pd.read_csv("data_setup/Data CSV/categories.csv")
    products = pd.read_csv("data_setup/Data CSV/products.csv")
    staff_raw = pd.read_csv("data_setup/Data CSV/staffs.csv")
    stocks = pd.read_csv("data_setup/Data CSV/stocks.csv")
    stores = pd.read_csv("data_setup/Data CSV/stores.csv")

    # API CSVs
    customers = pd.read_csv("csvs_from_api/customers.csv")
    order_items = pd.read_csv("csvs_from_api/order_items.csv")
    orders = pd.read_csv("csvs_from_api/orders.csv")

    return {
        "brands": brands,
        "categories": categories,
        "products": products,
        "staff_raw": staff_raw,  # raw, becomes 'staff' in transform
        "stocks": stocks,
        "stores": stores,
        "customers": customers,
        "order_items": order_items,
        "orders": orders,
    }


# -------------------- TRANSFORM --------------------

def transform(
    raw: Dict[str, pd.DataFrame]
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Dict[str, Any]]]:
    brands = raw["brands"].copy()
    categories = raw["categories"].copy()
    products = raw["products"].copy()
    customers = raw["customers"].copy()
    stores = raw["stores"].copy()
    orders = raw["orders"].copy()
    order_items = raw["order_items"].copy()
    stocks = raw["stocks"].copy()
    staff = raw["staff_raw"].copy()

    # Column renames for consistency
    # - stores.name -> store_name
    if "name" in stores.columns:
        stores = stores.rename(columns={"name": "store_name"})
    # - orders.store -> store_name; orders.staff_name -> staff_first_name
    orders = orders.rename(
        columns={"store": "store_name", "staff_name": "staff_first_name"}
    )
    # - staff: table 'staffs' -> 'staff'; name -> staff_first_name; last_name -> staff_last_name
    staff = staff.rename(
        columns={"name": "staff_first_name", "last_name": "staff_last_name"}
    )
    # If any legacy column in stocks uses 'store' or 'name', normalize to store_name
    if "store" in stocks.columns and "store_name" not in stocks.columns:
        stocks = stocks.rename(columns={"store": "store_name"})
    if "name" in stocks.columns and "store_name" not in stocks.columns:
        stocks = stocks.rename(columns={"name": "store_name"})

    # Datetime parsing for orders
    if "order_date" in orders.columns:
        orders["order_date"] = pd.to_datetime(
            orders["order_date"], format="%d/%m/%Y"
        )
    if "required_date" in orders.columns:
        orders["required_date"] = pd.to_datetime(
            orders["required_date"], format="%d/%m/%Y"
        )
    if "shipped_date" in orders.columns:
        orders["shipped_date"] = pd.to_datetime(
            orders["shipped_date"], format="%d/%m/%Y", errors="coerce"
        )

    # Normalize strings
    products = normalize_strings(products, ["product_name"])
    customers = normalize_strings(customers, ["first_name", "last_name", "email"])
    stores = normalize_strings(stores, ["store_name", "email"])
    staff = normalize_strings(
        staff, ["staff_first_name", "staff_last_name", "email", "store_name"]
    )
    orders = normalize_strings(
        orders, ["store_name", "staff_first_name", "order_status"]
    )
    stocks = normalize_strings(stocks, ["store_name"])

    # order_items: ensure item_id exists as 1..n per order
    if "item_id" not in order_items.columns or order_items["item_id"].isna().any():
        order_items = order_items.copy()
        order_items["item_id"] = (
            order_items.sort_values(["order_id", "product_id"])
            .groupby("order_id")
            .cumcount()
            + 1
        )

    # stocks: merge duplicates by summing quantity
    if {"store_name", "product_id", "quantity"}.issubset(stocks.columns):
        stocks = (
            stocks.groupby(
                ["store_name", "product_id"], as_index=False, dropna=False
            )
            .agg({"quantity": "sum"})
            .reset_index(drop=True)
        )

    # DTYPE map with new consistent names
    dtypes: Dict[str, Dict[str, Any]] = {
        "brands": {
            "brand_id": types.Integer(),
            "brand_name": types.String(VARCHAR_LEN),
        },
        "categories": {
            "category_id": types.Integer(),
            "category_name": types.String(VARCHAR_LEN),
        },
        "products": {
            "product_id": types.Integer(),
            "product_name": types.String(VARCHAR_LEN),
            "brand_id": types.Integer(),
            "category_id": types.Integer(),
            "model_year": types.Integer(),
            "listed_price": types.Float(),
        },
        "customers": {
            "customer_id": types.Integer(),
            "first_name": types.String(VARCHAR_LEN),
            "last_name": types.String(VARCHAR_LEN),
            "phone": types.String(50),
            "email": types.String(VARCHAR_LEN),
            "street": types.String(VARCHAR_LEN),
            "city": types.String(VARCHAR_LEN),
            "state": types.String(50),
            "zip_code": types.String(20),
        },
        "stores": {
            "store_name": types.String(VARCHAR_LEN),
            "phone": types.String(50),
            "email": types.String(VARCHAR_LEN),
            "street": types.String(VARCHAR_LEN),
            "city": types.String(VARCHAR_LEN),
            "state": types.String(50),
            "zip_code": types.String(20),
        },
        "staff": {
            "staff_first_name": types.String(VARCHAR_LEN),
            "staff_last_name": types.String(VARCHAR_LEN),
            "email": types.String(VARCHAR_LEN),
            "phone": types.String(50),
            "active": types.Boolean(),
            "store_name": types.String(VARCHAR_LEN),
            "manager_id": types.Integer(),
        },
        "orders": {
            "order_id": types.Integer(),
            "customer_id": types.Integer(),
            "order_status": types.String(50),
            "order_date": types.DateTime(),
            "required_date": types.DateTime(),
            "shipped_date": types.DateTime(),
            "store_name": types.String(VARCHAR_LEN),
            "staff_first_name": types.String(VARCHAR_LEN),
        },
        "order_items": {
            "order_id": types.Integer(),
            "item_id": types.Integer(),
            "product_id": types.Integer(),
            "quantity": types.Integer(),
            "list_price": types.Float(),
            "discount": types.Float(),
        },
        "stocks": {
            "product_id": types.Integer(),
            "store_name": types.String(VARCHAR_LEN),
            "quantity": types.Integer(),
        },
    }

    dfs: Dict[str, pd.DataFrame] = {
        "brands": brands,
        "categories": categories,
        "products": products,
        "customers": customers,
        "stores": stores,
        "staff": staff,
        "orders": orders,
        "order_items": order_items,
        "stocks": stocks,
    }

    return dfs, dtypes


# -------------------- LOAD (per-table helpers) --------------------

def load_brands(df: pd.DataFrame, dtypes: Dict[str, Dict[str, Any]]) -> None:
    load_to_sql(
        df=df,
        table_name="brands",
        primary_key="brand_id",
        dtype=dtypes["brands"],
    )


def load_categories(df: pd.DataFrame, dtypes: Dict[str, Dict[str, Any]]) -> None:
    load_to_sql(
        df=df,
        table_name="categories",
        primary_key="category_id",
        dtype=dtypes["categories"],
    )


def load_products(df: pd.DataFrame, dtypes: Dict[str, Dict[str, Any]]) -> None:
    load_to_sql(
        df=df,
        table_name="products",
        primary_key="product_id",
        foreign_keys=[
            {
                "columns": ["brand_id"],
                "ref_table": "brands",
                "ref_columns": ["brand_id"],
                "on_delete": "RESTRICT",
                "on_update": "CASCADE",
            },
            {
                "columns": ["category_id"],
                "ref_table": "categories",
                "ref_columns": ["category_id"],
                "on_delete": "RESTRICT",
                "on_update": "CASCADE",
            },
        ],
        dtype=dtypes["products"],
    )


def load_customers(df: pd.DataFrame, dtypes: Dict[str, Dict[str, Any]]) -> None:
    load_to_sql(
        df=df,
        table_name="customers",
        primary_key="customer_id",
        dtype=dtypes["customers"],
    )


def load_stores(df: pd.DataFrame, dtypes: Dict[str, Dict[str, Any]]) -> None:
    load_to_sql(
        df=df,
        table_name="stores",
        primary_key="store_name",
        dtype=dtypes["stores"],
    )


def load_staff(df: pd.DataFrame, dtypes: Dict[str, Dict[str, Any]]) -> None:
    load_to_sql(
        df=df,
        table_name="staff",
        unique=[("staff_first_name", "staff_last_name", "email", "store_name")],
        dtype=dtypes["staff"],
    )
    # Note: no FK for staff.store_name or staff_first_name


def load_orders(df: pd.DataFrame, dtypes: Dict[str, Dict[str, Any]]) -> None:
    load_to_sql(
        df=df,
        table_name="orders",
        primary_key="order_id",
        foreign_keys=[
            {
                "columns": ["customer_id"],
                "ref_table": "customers",
                "ref_columns": ["customer_id"],
                "on_delete": "RESTRICT",
                "on_update": "CASCADE",
            },
            {
                "columns": ["store_name"],
                "ref_table": "stores",
                "ref_columns": ["store_name"],
                "on_delete": "RESTRICT",
                "on_update": "CASCADE",
            },
            # No FK for staff_first_name
        ],
        dtype=dtypes["orders"],
    )


def load_order_items(df: pd.DataFrame, dtypes: Dict[str, Dict[str, Any]]) -> None:
    load_to_sql(
        df=df,
        table_name="order_items",
        primary_key=["order_id", "item_id"],
        foreign_keys=[
            {
                "columns": ["order_id"],
                "ref_table": "orders",
                "ref_columns": ["order_id"],
                "on_delete": "CASCADE",
                "on_update": "CASCADE",
            },
            {
                "columns": ["product_id"],
                "ref_table": "products",
                "ref_columns": ["product_id"],
                "on_delete": "RESTRICT",
                "on_update": "CASCADE",
            },
        ],
        dtype=dtypes["order_items"],
    )


def load_stocks(df: pd.DataFrame, dtypes: Dict[str, Dict[str, Any]]) -> None:
    load_to_sql(
        df=df,
        table_name="stocks",
        primary_key=["store_name", "product_id"],
        foreign_keys=[
            {
                "columns": ["store_name"],
                "ref_table": "stores",
                "ref_columns": ["store_name"],
                "on_delete": "RESTRICT",
                "on_update": "CASCADE",
            },
            {
                "columns": ["product_id"],
                "ref_table": "products",
                "ref_columns": ["product_id"],
                "on_delete": "RESTRICT",
                "on_update": "CASCADE",
            },
        ],
        dtype=dtypes["stocks"],
    )


# -------------------- LOAD (orchestration) --------------------

def load(dfs: Dict[str, pd.DataFrame], dtypes: Dict[str, Dict[str, Any]]) -> None:
    print("Loading brands...")
    load_brands(dfs["brands"], dtypes)
    print("Loading categories...")
    load_categories(dfs["categories"], dtypes)
    print("Loading customers...")
    load_customers(dfs["customers"], dtypes)
    print("Loading stores...")
    load_stores(dfs["stores"], dtypes)
    print("Loading products...")
    load_products(dfs["products"], dtypes)
    print("Loading staff...")
    load_staff(dfs["staff"], dtypes)
    print("Loading orders...")
    load_orders(dfs["orders"], dtypes)
    print("Loading order_items...")
    load_order_items(dfs["order_items"], dtypes)
    print("Loading stocks...")
    load_stocks(dfs["stocks"], dtypes)
    print("\nAll data loaded successfully!")


# -------------------- RUN ETL --------------------

if __name__ == "__main__":
    raw_dfs = extract()
    dfs, dtypes = transform(raw_dfs)
    load(dfs, dtypes)