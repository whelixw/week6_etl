from typing import Any, Dict, Iterable, List, Optional, Sequence, Union
import pandas as pd
from sqlalchemy import text, types, create_engine


# Load the CSVs from the "data_setup/Data CSV" folder with relative paths
brands = pd.read_csv('data_setup/Data CSV/brands.csv')
categories = pd.read_csv('data_setup/Data CSV/categories.csv')
products = pd.read_csv('data_setup/Data CSV/products.csv')
staffs = pd.read_csv('data_setup/Data CSV/staffs.csv')
stocks = pd.read_csv('data_setup/Data CSV/stocks.csv')
stores = pd.read_csv('data_setup/Data CSV/stores.csv')

# Load the CSVs from the "csvs_from_api" folder
customers = pd.read_csv('csvs_from_api/customers.csv')
order_items = pd.read_csv('csvs_from_api/order_items.csv')
orders = pd.read_csv('csvs_from_api/orders.csv')

# Fix_datetime for orders
orders['order_date'] = pd.to_datetime(orders['order_date'], format='%d/%m/%Y')
orders['required_date'] = pd.to_datetime(orders['required_date'], format='%d/%m/%Y')
orders['shipped_date'] = pd.to_datetime(orders['shipped_date'], format='%d/%m/%Y', errors='coerce') # Fixed 'shpped_date' to 'shipped_date' if that was a typo

engine = create_engine(
    "mysql+pymysql://app:AppPass!1@127.0.0.1:3306/mydb",
    pool_pre_ping=True,
)

VARCHAR_LEN = 191  # safer for utf8mb4 indexed columns in MySQL


def _q(name: str) -> str:
    return f"`{name}`"


def _cols(cols: Sequence[str]) -> str:
    return ", ".join(_q(c) for c in cols)


def _constraint_name(prefix: str, table: str, cols: Sequence[str]) -> str:
    base = f"{prefix}_{table}_{'_'.join(cols)}"
    # Keep it short-ish for MySQL
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
    foreign_keys: Optional[
        List[Dict[str, Any]]
    ] = None,  # dict: columns, ref_table, ref_columns, on_delete, on_update
    unique: Optional[List[Sequence[str]]] = None,
    dtype: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Writes df to MySQL as table_name (replacing), then adds constraints.

    - primary_key: str or list[str]
    - foreign_keys: list of dicts with keys:
        - columns: list[str]
        - ref_table: str
        - ref_columns: list[str]
        - on_delete: str (optional)
        - on_update: str (optional)
    - unique: list of unique key column lists
    - dtype: pandas to_sql dtype mapping (SQLAlchemy types)
    """
    if foreign_keys is None:
        foreign_keys = []
    if unique is None:
        unique = []

    df = df.drop_duplicates()
    df = df.convert_dtypes().infer_objects()

    with engine.begin() as conn:
        # Create/replace table + data
        df.to_sql(
            table_name,
            con=conn,
            if_exists="replace",
            index=False,
            dtype=dtype,
            method=None,
        )

        # Add unique constraints first (if any)
        for cols in unique:
            cols = list(cols)
            uc_name = _constraint_name("uq", table_name, cols)
            conn.execute(
                text(
                    f"ALTER TABLE {_q(table_name)} "
                    f"ADD CONSTRAINT {_q(uc_name)} UNIQUE ({_cols(cols)})"
                )
            )

        # Add primary key
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

        # Add foreign keys
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


# ---------- DTYPE MAPS (ensure FK-able VARCHAR, not TEXT) ----------

DTYPES = {
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
        "model_year": types.Integer(),  # or types.Integer() if a 4-digit year
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
        "name": types.String(VARCHAR_LEN), # Corrected: Stores table uses 'name' as PK
        "phone": types.String(50),
        "email": types.String(VARCHAR_LEN),
        "street": types.String(VARCHAR_LEN),
        "city": types.String(VARCHAR_LEN),
        "state": types.String(50),
        "zip_code": types.String(20),
    },
    "staffs": {
        "name": types.String(VARCHAR_LEN), # Corrected: Staffs table uses 'name'
        "last_name": types.String(VARCHAR_LEN),
        "email": types.String(VARCHAR_LEN),
        "phone": types.String(50),
        "active": types.Boolean(),
        "store_name": types.String(VARCHAR_LEN), # This column remains 'store_name' in the staffs table, referencing stores.name
        "manager_id": types.Integer(),
    },
    "orders": {
        "order_id": types.Integer(),
        "customer_id": types.Integer(),
        "order_status": types.String(50),
        "order_date": types.DateTime(),
        "required_date": types.DateTime(),
        "shipped_date": types.DateTime(),
        "store": types.String(VARCHAR_LEN), # Corrected: Orders table uses 'store'
        "staff_name": types.String(VARCHAR_LEN),
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
        "store_name": types.String(VARCHAR_LEN), # This column remains 'store_name' in the stocks table, referencing stores.name
        "quantity": types.Integer(),
    },
}


# ---------- PER-TABLE LOADERS ----------

def load_brands(df: pd.DataFrame) -> None:
    load_to_sql(
        df=df,
        table_name="brands",
        primary_key="brand_id",
        dtype=DTYPES["brands"],
    )


def load_categories(df: pd.DataFrame) -> None:
    load_to_sql(
        df=df,
        table_name="categories",
        primary_key="category_id",
        dtype=DTYPES["categories"],
    )


def load_products(df: pd.DataFrame) -> None:
    df = df.copy()
    df = normalize_strings(df, ["product_name"])
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
        dtype=DTYPES["products"],
    )


def load_customers(df: pd.DataFrame) -> None:
    # Ensure input DF for customers has 'first_name' and 'last_name' as per the DTYPE and table structure
    df = normalize_strings(df, ["first_name", "last_name", "email"])
    load_to_sql(
        df=df,
        table_name="customers",
        primary_key="customer_id",
        dtype=DTYPES["customers"],
    )


def load_stores(df: pd.DataFrame) -> None:
    # Expects the input DataFrame 'df' for stores to have a 'name' column
    df = normalize_strings(df, ["name", "email"])
    load_to_sql(
        df=df,
        table_name="stores",
        primary_key="name", # Corrected: Primary key is 'name'
        dtype=DTYPES["stores"],
    )


def load_staffs(df: pd.DataFrame) -> None:
    # Expects the input DataFrame 'df' for staffs to have a 'name' (for first name) and 'store_name' column
    df = normalize_strings(df, ["name", "last_name", "email", "store_name"]) # Corrected: uses 'name' for first name
    load_to_sql(
        df=df,
        table_name="staffs",
        # Added unique constraint on a combination of columns to serve as a pseudo-PK for staff
        unique=[("name", "last_name", "email", "store_name")],
        dtype=DTYPES["staffs"],
    )
    # Note: manager_id cannot be an FK here because there's no staff_id.
    # And store_name cannot be an FK because staffs has no PK and it's not strictly unique by itself.


def load_orders(df: pd.DataFrame) -> None:
    # Expects the input DataFrame 'df' for orders to have a 'store' and 'staff_name' column
    df = normalize_strings(df, ["store", "staff_name", "order_status"]) # Corrected: normalize 'store'
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
                "columns": ["store"],       # Corrected: This is the column in the 'orders' table
                "ref_table": "stores",
                "ref_columns": ["name"],    # This is the PK column in the 'stores' table
                "on_delete": "RESTRICT",
                "on_update": "CASCADE",
            },
            # No FK for staff_name (string) to staffs since no unique key exists there
        ],
        dtype=DTYPES["orders"],
    )


def load_order_items(df: pd.DataFrame) -> None:
    df = df.copy()
    # Ensure item_id exists per order as a 1..n line number if missing
    if "item_id" not in df.columns or df["item_id"].isna().any():
        df["item_id"] = (
            df.sort_values(["order_id", "product_id"])
            .groupby("order_id")
            .cumcount()
            + 1
        )
    # Composite PK (order_id, item_id)
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
        dtype=DTYPES["order_items"],
    )


def load_stocks(df: pd.DataFrame) -> None:
    df = df.copy()
    # Expects the input DataFrame 'df' for stocks to have a 'store_name' column
    df = normalize_strings(df, ["store_name"])
    # Merge duplicates (store_name, product_id) by summing quantities
    df = (
        df.groupby(["store_name", "product_id"], as_index=False, dropna=False)
        .agg({"quantity": "sum"})
        .reset_index(drop=True)
    )
    # Composite PK (store_name, product_id)
    load_to_sql(
        df=df,
        table_name="stocks",
        primary_key=["store_name", "product_id"], # This is the column in the 'stocks' table
        foreign_keys=[
            {
                "columns": ["store_name"],  # This is the column in the 'stocks' table
                "ref_table": "stores",
                "ref_columns": ["name"],    # This is the PK column in the 'stores' table
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
        dtype=DTYPES["stocks"],
    )


# ---------- Orchestration helper ----------

def load_all(
    brands_df: pd.DataFrame,
    categories_df: pd.DataFrame,
    products_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    stores_df: pd.DataFrame, # Expects a 'name' column in this DataFrame
    staffs_df: pd.DataFrame, # Expects a 'name' column (for first name) in this DataFrame
    orders_df: pd.DataFrame, # Expects a 'store' column (for store reference) in this DataFrame
    order_items_df: pd.DataFrame,
    stocks_df: pd.DataFrame,
) -> None:
    # Load in dependency order
    print("Loading brands...")
    load_brands(brands_df)
    print("Loading categories...")
    load_categories(categories_df)
    print("Loading customers...")
    load_customers(customers_df)
    print("Loading stores...")
    load_stores(stores_df)
    print("Loading products...")
    load_products(products_df) # depends on brands, categories
    print("Loading staffs...")
    load_staffs(staffs_df) # depends on stores.name (implicitly, not via FK due to your schema)
    print("Loading orders...")
    load_orders(orders_df) # depends on customers, stores.name, staffs (implicitly)
    print("Loading order_items...")
    load_order_items(order_items_df) # depends on orders, products
    print("Loading stocks...")
    load_stocks(stocks_df) # depends on stores.name, products
    print("\nAll data loaded successfully!")


# Execute the loading process
load_all(
    brands,
    categories,
    products,
    customers,
    stores,
    staffs,
    orders,
    order_items,
    stocks,
)