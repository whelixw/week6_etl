from typing import Optional
from sqlalchemy import text, create_engine
from sqlalchemy.exc import IntegrityError

from utils import (
    _q,
    archive_inputs,
    build_dataset_dtypes,
    floor_to_minute_utc,
    get_archive_root,
    get_engine,
    get_load_sequence,
    get_source_paths,
    get_tables_map,
    get_key_columns,
    load_config,
    transform_dataset,
)

cfg = load_config("config.yaml")
engine = get_engine(cfg)




def run_sql_test_transactional(description: str, sql_command: str, expected_error: bool = False,
                               setup_sql: Optional[str] = None, verification_sql: Optional[str] = None):
    """
    Runs a SQL test within a transaction that is always rolled back.
    No permanent changes are committed to the database.

    :param description: Description of the test.
    :param sql_command: The SQL command to execute (the main test action).
    :param expected_error: True if an IntegrityError is expected.
    :param setup_sql: Optional SQL to run before the main command, within the same transaction. Can contain multiple statements.
    :param verification_sql: Optional SQL to run after the main command to verify changes (within transaction).
    """
    print(f"\nTest: {description}")

    conn = None
    try:
        conn = engine.connect()
        trans = conn.begin()  # Start a single transaction for the whole test case

        # Run setup SQL if provided (split into multiple statements)
        if setup_sql:
            for stmt in setup_sql.split(';'):
                stripped_stmt = stmt.strip()
                if stripped_stmt:
                    conn.execute(text(stripped_stmt))

        # Run the main test command
        conn.execute(text(sql_command))

        # Run verification SQL if provided
        if verification_sql:
            # Assuming verification_sql is a single SELECT statement
            result = conn.execute(text(verification_sql)).scalar()
            print(f"  Verification result: {result}")

        if expected_error:
            print(f"  ❌ FAILED: Expected an error, but query succeeded.")
        else:
            print(f"  ✅ PASSED: Query executed successfully (changes will be rolled back).")

    except IntegrityError as e:
        if expected_error:
            print(f"  ✅ PASSED: Caught expected IntegrityError: {e} (transaction will be rolled back).")
        else:
            print(f"  ❌ FAILED: Unexpected IntegrityError: {e} (transaction will be rolled back).")
    except Exception as e:
        print(f"  ❌ FAILED: An unexpected error occurred: {e} (transaction will be rolled back).")
    finally:
        if conn and not conn.closed:
            try:
                trans.rollback()  # ALWAYS rollback for transactional tests
            except Exception as e:
                print(f"  ⚠️ WARNING: Failed to rollback transaction: {e}")
            finally:
                conn.close()  # Close the connection


# --- 1. Test Explicit Foreign Keys (Violations) ---

# Get existing IDs for testing
try:
    with engine.connect() as conn:
        existing_brand_id = conn.execute(text("SELECT brand_id FROM brands ORDER BY brand_id LIMIT 1")).scalar()
        existing_category_id = conn.execute(
            text("SELECT category_id FROM categories ORDER BY category_id LIMIT 1")).scalar()
        existing_customer_id = conn.execute(
            text("SELECT customer_id FROM customers ORDER BY customer_id LIMIT 1")).scalar()
        existing_store_name = conn.execute(text("SELECT name FROM stores ORDER BY name LIMIT 1")).scalar()
        existing_product_id = conn.execute(text("SELECT product_id FROM products ORDER BY product_id LIMIT 1")).scalar()
        existing_order_id = conn.execute(text("SELECT order_id FROM orders ORDER BY order_id LIMIT 1")).scalar()

    print(
        f"Found existing brand_id: {existing_brand_id}, category_id: {existing_category_id}, customer_id: {existing_customer_id}, "
        f"store_name: '{existing_store_name}', product_id: {existing_product_id}, order_id: {existing_order_id}")
except Exception as e:
    print(f"Could not retrieve existing IDs for testing. Please ensure tables are populated. Error: {e}")
    exit()

# Test 1.1: Insert into products with non-existent brand_id
run_sql_test_transactional(
    "Insert product with non-existent brand_id",
    f"INSERT INTO products (product_id, product_name, brand_id, category_id, model_year, list_price) "
    f"VALUES (999999, 'Test Product No Brand', 0, {existing_category_id}, '2020', 100.00);",
    expected_error=True
)

# Test 1.2: Insert into orders with non-existent customer_id
run_sql_test_transactional(
    "Insert order with non-existent customer_id",
    f"INSERT INTO orders (order_id, customer_id, order_status, order_date, store, staff_name) "
    f"VALUES (999999, 0, 'Pending', CURDATE(), '{existing_store_name}', 'Test Staff');",
    expected_error=True
)

# Test 1.3: Insert into orders with non-existent store name
run_sql_test_transactional(
    "Insert order with non-existent store name",
    f"INSERT INTO orders (order_id, customer_id, order_status, order_date, store, staff_name) "
    f"VALUES (999998, {existing_customer_id}, 'Pending', CURDATE(), 'NonExistentStore', 'Test Staff');",
    expected_error=True
)

# Test 1.4: Insert into stocks with non-existent product_id
run_sql_test_transactional(
    "Insert stock with non-existent product_id",
    f"INSERT INTO stocks (product_id, store_name, quantity) VALUES (0, '{existing_store_name}', 5);",
    expected_error=True
)

# Test 1.5: Insert into order_items with non-existent order_id
run_sql_test_transactional(
    "Insert order_item with non-existent order_id",
    f"INSERT INTO order_items (order_id, item_id, product_id, quantity, list_price, discount) "
    f"VALUES (0, 1, {existing_product_id}, 1, 10.00, 0.0);",
    expected_error=True
)

# Test 1.6: Insert into stocks with non-existent store_name
run_sql_test_transactional(
    "Insert stock with non-existent store_name",
    f"INSERT INTO stocks (product_id, store_name, quantity) VALUES ({existing_product_id}, 'FakeStore', 5);",
    expected_error=True
)

# Test 1.7: Composite PK violation in order_items
run_sql_test_transactional(
    "Insert duplicate (order_id, item_id) in order_items",
    f"INSERT INTO order_items (order_id, item_id, product_id, quantity, list_price, discount) "
    f"SELECT order_id, item_id, product_id, quantity, list_price, discount FROM order_items LIMIT 1;",
    expected_error=True
)

# Test 1.8: Composite PK violation in stocks
run_sql_test_transactional(
    "Insert duplicate (store_name, product_id) in stocks",
    f"INSERT INTO stocks (store_name, product_id, quantity) "
    f"SELECT store_name, product_id, quantity FROM stocks LIMIT 1;",
    expected_error=True
)

# --- 2. Test Deletion of Referenced Primary Keys (RESTRICT) ---

# Test 2.1: Delete a brand that is referenced by a product (should be RESTRICTED)
run_sql_test_transactional(
    "Delete a brand referenced by products",
    f"DELETE FROM brands WHERE brand_id = {existing_brand_id};",
    expected_error=True
)

# Test 2.2: Delete a customer referenced by orders (should be RESTRICTED)
run_sql_test_transactional(
    "Delete a customer referenced by orders",
    f"DELETE FROM customers WHERE customer_id = {existing_customer_id};",
    expected_error=True
)

# Test 2.3: Delete a store referenced by orders or stocks (should be RESTRICTED)
run_sql_test_transactional(
    "Delete a store referenced by orders or stocks",
    f"DELETE FROM stores WHERE name = '{existing_store_name}';",
    expected_error=True
)

# Test 2.4: Delete an order referenced by order_items (ON DELETE CASCADE)
test_order_id_for_cascade = 999996  # A unique ID for this test
run_sql_test_transactional(
    "Delete an order referenced by order_items (ON DELETE CASCADE)",
    f"DELETE FROM orders WHERE order_id = {test_order_id_for_cascade};",
    setup_sql=f"""
        INSERT INTO orders (order_id, customer_id, order_status, order_date, store, staff_name) 
        VALUES ({test_order_id_for_cascade}, {existing_customer_id}, 'Test', CURDATE(), '{existing_store_name}', 'Test Staff');
        INSERT INTO order_items (order_id, item_id, product_id, quantity, list_price, discount) 
        VALUES ({test_order_id_for_cascade}, 1, {existing_product_id}, 1, 10.00, 0.0);
        INSERT INTO order_items (order_id, item_id, product_id, quantity, list_price, discount) 
        VALUES ({test_order_id_for_cascade}, 2, {existing_product_id}, 2, 20.00, 0.0);
    """,
    verification_sql=f"SELECT COUNT(*) FROM order_items WHERE order_id = {test_order_id_for_cascade}",
    # Should be 0 after cascade
    expected_error=False
)

# --- 3. Test Update of Referenced Primary Keys (CASCADE) ---

# Test 3.1: Update a brand_id (ON UPDATE CASCADE)
test_brand_id_old = 999990
test_brand_id_new = 999991
test_product_id_for_brand_update = 999990
run_sql_test_transactional(
    "Update a brand_id referenced by products (ON UPDATE CASCADE)",
    f"UPDATE brands SET brand_id = {test_brand_id_new} WHERE brand_id = {test_brand_id_old};",
    setup_sql=f"""
        INSERT INTO brands (brand_id, brand_name) VALUES ({test_brand_id_old}, 'Test Brand for Update');
        INSERT INTO products (product_id, product_name, brand_id, category_id, model_year, list_price) 
        VALUES ({test_product_id_for_brand_update}, 'Product for Brand Update', {test_brand_id_old}, {existing_category_id}, '2020', 100.00);
    """,
    verification_sql=f"SELECT COUNT(*) FROM products WHERE brand_id = {test_brand_id_new} AND product_id = {test_product_id_for_brand_update}",
    # Should be 1
    expected_error=False
)

# Test 3.2: Update a store name (ON UPDATE CASCADE)
test_store_name_old = 'TestStoreOldName'
test_store_name_new = 'TestStoreNewName'
test_order_id_for_store_update = 999997
test_product_id_for_store_stock = existing_product_id
run_sql_test_transactional(
    "Update a store name referenced by orders/stocks (ON UPDATE CASCADE)",
    f"UPDATE stores SET name = '{test_store_name_new}' WHERE name = '{test_store_name_old}';",
    setup_sql=f"""
        INSERT INTO stores (name, phone, email) VALUES ('{test_store_name_old}', '123-456-7890', 'testold@example.com');
        INSERT INTO orders (order_id, customer_id, order_status, order_date, store, staff_name) 
        VALUES ({test_order_id_for_store_update}, {existing_customer_id}, 'Completed', CURDATE(), '{test_store_name_old}', 'Any Staff');
        INSERT INTO stocks (product_id, store_name, quantity) 
        VALUES ({test_product_id_for_store_stock}, '{test_store_name_old}', 10);
    """,
    verification_sql=f"""
        SELECT 
            (SELECT COUNT(*) FROM orders WHERE store = '{test_store_name_new}' AND order_id = {test_order_id_for_store_update}) +
            (SELECT COUNT(*) FROM stocks WHERE store_name = '{test_store_name_new}' AND product_id = {test_product_id_for_store_stock})
    """,  # Should be 2 if both cascaded
    expected_error=False
)

# --- 4. Test "Soft" Relations (Joins and Data Integrity Checks for Staffs) ---

print("\n--- Testing Soft Relations: Staffs ---")

# Test 4.1: Identify orders with staff_name that don't match any staff record
print("Finding orders with staff_name/store combination not matching any staff record:")
# This is a read-only query, so no need for setup/teardown in a transactional test.
with engine.connect() as conn:
    query = text("""
        SELECT
            o.order_id,
            o.staff_name AS order_staff_name,
            o.store AS order_store,
            s.name AS staffs_name,
            s.last_name AS staffs_last_name,
            s.store_name AS staffs_store_name
        FROM
            orders o
        LEFT JOIN
            staffs s ON o.staff_name = s.name AND o.store = s.store_name
        WHERE
            s.name IS NULL
        LIMIT 10;
    """)
    unmatched_staff_orders = conn.execute(query).fetchall()

    if unmatched_staff_orders:
        print("  ⚠️ WARNING: The following orders have 'staff_name' and 'store' that do not match a 'staffs' record:")
        for row in unmatched_staff_orders:
            print(
                f"    Order ID: {row.order_id}, Staff Name in Order: '{row.order_staff_name}', Store in Order: '{row.order_store}'")
        print(
            "  (This is expected behavior for soft relations, but indicates potential data inconsistencies in source data)")
    else:
        print(
            "  ✅ PASSED: All orders' staff_name/store combination match at least one staff record (based on first name and store).")

# Test 4.2: Identify staff records that are never linked to an order
print("\nFinding staff records never linked to any order:")
with engine.connect() as conn:
    query = text("""
        SELECT
            s.name,
            s.last_name,
            s.store_name
        FROM
            staffs s
        LEFT JOIN
            orders o ON s.name = o.staff_name AND s.store_name = o.store
        WHERE
            o.order_id IS NULL
        LIMIT 10;
    """)
    unlinked_staff = conn.execute(query).fetchall()

    if unlinked_staff:
        print("  ℹ️ INFO: The following staff members are not linked to any orders:")
        for row in unlinked_staff:
            print(f"    Staff: {row.name} {row.last_name} (Store: {row.store_name})")
        print("  (This is informational and not an error unless all staff should have orders.)")
    else:
        print("  ✅ PASSED: All staff members appear to be linked to at least one order.")

# --- 5. Test Unique Constraint on Staffs ---

# Test 5.1: Insert a staff member that violates the unique constraint
# (name, last_name, email, store_name)
print("\n--- Testing Unique Constraint on Staffs ---")
with engine.connect() as conn:
    # Get an existing staff member's details
    existing_staff_row = conn.execute(
        text("SELECT name, last_name, email, store_name FROM staffs ORDER BY name LIMIT 1")).fetchone()
    if existing_staff_row:
        name, last_name, email, store_name = existing_staff_row
        run_sql_test_transactional(
            f"Insert duplicate staff: '{name} {last_name}', '{email}', '{store_name}'",
            f"INSERT INTO staffs (name, last_name, email, phone, active, store_name, manager_id) "
            f"VALUES ('{name}', '{last_name}', '{email}', '555-1234', TRUE, '{store_name}', NULL);",
            expected_error=True
        )
    else:
        print("  Skipped: No staff records found to test unique constraint.")
# --- NEW TEST: Select staffs who sold to 'Debra' ---
print("\n--- NEW TEST: Select staffs who sold to 'Debra' ---")
debra_staffs_query = """
    SELECT DISTINCT
        st.name AS staff_first_name,
        st.last_name AS staff_last_name,
        s.name AS store_name -- Renamed 's.name' for clarity as it's the store's name
    FROM
        customers c
    JOIN
        orders o ON c.customer_id = o.customer_id
    JOIN
        staffs st ON o.staff_name = st.name AND o.store = st.store_name
    JOIN
        stores s ON o.store = s.name -- Added to get the store's official name if needed for staffs table
    WHERE
        c.first_name = 'Debra';"""

with engine.connect() as conn:
    print(f"\nQuerying for staffs who sold to 'Debra':\n{debra_staffs_query}")
    results = conn.execute(text(debra_staffs_query)).fetchall()

    if results:
        print("  ✅ PASSED: Found staff members who sold to 'Debra':")
        for row in results:
            print(f"    Staff: {row.staff_first_name} {row.staff_last_name} (Store: {row.store_name})")
    else:
        print("  ℹ️ INFO: No staff members found who sold to 'Debra' in the dataset. This might be due to no 'Debra' customers or no orders from them.")
        # You might want to add a setup_sql here to ensure a 'Debra' exists and has an order if you want to guarantee a hit.
print("\n--- Relationship Tests Complete ---")

# --- 5. Test Unique Constraint on Staffs ---

# Test 5.1: Insert a staff member that violates the unique constraint
# (name, last_name, email, store_name)
print("\n--- Testing Unique Constraint on Staffs ---")
with engine.connect() as conn:
    # Get an existing staff member's details
    existing_staff_row = conn.execute(text("SELECT name, last_name, email, store_name FROM staffs ORDER BY name LIMIT 1")).fetchone()
    if existing_staff_row:
        name, last_name, email, store_name = existing_staff_row
        run_sql_test_transactional(
            f"Insert duplicate staff: '{name} {last_name}', '{email}', '{store_name}'",
            f"INSERT INTO staffs (name, last_name, email, phone, active, store_name, manager_id) "
            f"VALUES ('{name}', '{last_name}', '{email}', '555-1234', TRUE, '{store_name}', NULL);",
            expected_error=True
        )
    else:
        print("  Skipped: No staff records found to test unique constraint.")

print("\n--- Relationship Tests Complete ---")


print("\n--- Bonus Test: Soft Relation vs. Hard Relation for Orders ---")

# Test 1: Insert order with non-existent staff_name (expected to succeed - highlights soft relation)
run_sql_test_transactional(
    "Insert order with non-existent staff (expected to succeed - no FK enforced)",
    f"INSERT INTO orders (order_id, customer_id, order_status, order_date, store, staff_name) "
    f"VALUES (999995, {existing_customer_id}, 'Completed', CURDATE(), '{existing_store_name}', 'NonExistentStaff_XY123');",
    expected_error=False
)

# Test 2: Insert staff with non-existent store_name (expected to succeed - highlights soft relation)
run_sql_test_transactional(
    "Insert order with non-existent staff (expected to fail - FK enforced)",
    f"INSERT INTO orders (order_id, customer_id, order_status, order_date, store, staff_name) "
    f"VALUES (999995, {existing_customer_id}, 'Completed', CURDATE(), 'fakestorename', 'NonExistentStaff_XY123');",
    expected_error=False
)