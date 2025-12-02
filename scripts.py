import pandas as pd

sales_df = pd.read_csv("https://raw.githubusercontent.com/gbuvoli/Datasets/refs/heads/main/Online_Sales.csv")
customers_df = pd.read_csv("https://raw.githubusercontent.com/gbuvoli/Datasets/refs/heads/main/CustomersData.csv")
discount_df = pd.read_csv("https://raw.githubusercontent.com/gbuvoli/Datasets/refs/heads/main/Discount_Coupon.csv")
marketing_df = pd.read_csv("https://raw.githubusercontent.com/gbuvoli/Datasets/refs/heads/main/Marketing_Spend.csv")
tax_df = pd.read_csv("https://raw.githubusercontent.com/gbuvoli/Datasets/refs/heads/main/Tax_amount.csv")

import sqlite3
import numpy as np
from datetime import timedelta

# ============================================
# 1. Conexión a la base de datos
# ============================================

DB_NAME = "ecommerce_demo.db"
conn = sqlite3.connect(DB_NAME)

# ============================================
# 2. Tabla customers (CustomerID, Gender, Location, signup_date)
# ============================================

# Nos quedamos con columnas básicas
customers = customers_df[["CustomerID", "Gender", "Location"]].copy()

# Parsear fechas de transacción para poder calcular primera compra
sales_tmp = sales_df.copy()
sales_tmp["Transaction_Date_dt"] = pd.to_datetime(
    sales_tmp["Transaction_Date"], format="%m/%d/%Y"
)

# Fecha de primera compra por cliente (solo para generar signup_date)
first_purchase = (
    sales_tmp
    .groupby("CustomerID")["Transaction_Date_dt"]
    .min()
    .reset_index()
    .rename(columns={"Transaction_Date_dt": "first_purchase_date"})
)

# Unimos a customers
customers = customers.merge(first_purchase, on="CustomerID", how="left")

# Simulamos signup_date como primera_compra - offset aleatorio [0, 90] días
np.random.seed(42)
offset_days = np.random.randint(0, 91, size=len(customers))
customers["signup_date"] = customers["first_purchase_date"] - pd.to_timedelta(offset_days, unit="D")

# Dejamos SOLO lo que queremos en el esquema final
customers = customers[["CustomerID", "Gender", "Location", "signup_date"]]

# Guardar en la DB
customers.to_sql("customers", conn, if_exists="replace", index=False)
print("Tabla 'customers' creada:", customers.shape)

# ============================================
# 3. Tabla sales
# ============================================

sales = sales_df[[
    "Transaction_ID",
    "CustomerID",
    "Transaction_Date",
    "Product_SKU",
    "Product_Description",
    "Product_Category",
    "Quantity",
    "Avg_Price",
    "Delivery_Charges",
    "Coupon_Status"
]].copy()

sales.to_sql("sales", conn, if_exists="replace", index=False)
print("Tabla 'sales' creada:", sales.shape)

# ============================================
# 4. Tabla discount_coupon
# ============================================

# Asumiendo columnas: Month, Product_Category, Coupon_Code, Discount_pct
discount_coupon = discount_df[[
    "Coupon_Code",
    "Product_Category",
    "Month",
    "Discount_pct"
]].copy()

discount_coupon.to_sql("discount_coupon", conn, if_exists="replace", index=False)
print("Tabla 'discount_coupon' creada:", discount_coupon.shape)

# ============================================
# 5. Tabla marketing_spend
# ============================================

marketing_spend = marketing_df[["Date", "Offline_Spend", "Online_Spend"]].copy()

marketing_spend.to_sql("marketing_spend", conn, if_exists="replace", index=False)
print("Tabla 'marketing_spend' creada:", marketing_spend.shape)

# ============================================
# 6. Tabla tax_amount
# ============================================

tax_amount = tax_df[["Product_Category", "GST"]].copy()

tax_amount.to_sql("tax_amount", conn, if_exists="replace", index=False)
print("Tabla 'tax_amount' creada:", tax_amount.shape)

# ============================================
# 7. Tabla events (para journeys / funnels)
#    - Sesiones con compra: funnel completo
#    - Sesiones sin compra: funnel truncado
# ============================================

events = []
event_id = 1

# Aseguramos columna datetime en sales_tmp
sales_tmp = sales_df.copy()
sales_tmp["Transaction_Date_dt"] = pd.to_datetime(
    sales_tmp["Transaction_Date"], format="%m/%d/%Y"
)

funnel_steps = ["page_view", "view_item", "add_to_cart", "begin_checkout", "purchase"]

# 7.1 Sesiones ligadas a transacciones reales (is_purchase_session = 1)
for idx, row in sales_tmp.iterrows():
    customer_id = row["CustomerID"]
    txn_id = row["Transaction_ID"]
    prod_cat = row["Product_Category"]
    purchase_time = row["Transaction_Date_dt"]
    session_id = f"txn_{txn_id}"

    # Creamos 5 eventos espaciados (-15, -10, -5, -2, 0 minutos)
    offsets = [-15, -10, -5, -2, 0]
    for step_name, minutes_offset in zip(funnel_steps, offsets):
        ev_time = purchase_time + timedelta(minutes=minutes_offset)
        events.append({
            "event_id": event_id,
            "customer_id": customer_id,
            "session_id": session_id,
            "event_name": step_name,
            "event_time": ev_time.strftime("%Y-%m-%d %H:%M:%S"),
            "product_category": prod_cat,
            "is_purchase_session": 1
        })
        event_id += 1

# 7.2 Sesiones adicionales sin compra (is_purchase_session = 0)
min_date = sales_tmp["Transaction_Date_dt"].min()
max_date = sales_tmp["Transaction_Date_dt"].max()
all_categories = sales_tmp["Product_Category"].unique()
customers_list = customers["CustomerID"].tolist()

np.random.seed(123)

for customer_id in customers_list:
    # 0 a 3 sesiones adicionales por cliente
    n_extra = np.random.randint(0, 4)

    for s in range(n_extra):
        # Tiempo aleatorio entre min y max
        rand_days = np.random.randint(0, (max_date - min_date).days + 1)
        rand_minutes = np.random.randint(0, 60 * 12)
        start_time = min_date + timedelta(days=rand_days, minutes=rand_minutes)

        # Último paso del funnel (0..3) SIN purchase
        last_step = np.random.choice([0, 1, 2, 3], p=[0.3, 0.3, 0.25, 0.15])

        prod_cat = np.random.choice(all_categories)
        session_id = f"cust{customer_id}_extra{s+1}"

        for step_index in range(last_step + 1):
            step_name = funnel_steps[step_index]
            ev_time = start_time + timedelta(minutes=step_index * 5)
            events.append({
                "event_id": event_id,
                "customer_id": customer_id,
                "session_id": session_id,
                "event_name": step_name,
                "event_time": ev_time.strftime("%Y-%m-%d %H:%M:%S"),
                "product_category": prod_cat,
                "is_purchase_session": 0
            })
            event_id += 1

events_df = pd.DataFrame(events)
events_df.to_sql("events", conn, if_exists="replace", index=False)
print("Tabla 'events' creada:", events_df.shape)

# ============================================
# 8. Cerrar conexión
# ============================================

conn.commit()
conn.close()
print(f"Base de datos '{DB_NAME}' creada y poblada correctamente.")
