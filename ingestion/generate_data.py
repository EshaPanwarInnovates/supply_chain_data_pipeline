
from faker import Faker
from pathlib import Path
import csv, random
from datetime import datetime, timedelta

fake = Faker(); random.seed(42)

ROOT = Path(__file__).resolve().parent.parent / 'data' / 'sample'
ROOT.mkdir(parents=True, exist_ok=True)

products=[]; suppliers=[]; warehouses=[]

for i in range(1,101):
    products.append([
        f'P{i:04d}',
        f'Product-{i:04d}',
        random.choice(['Electronics','Home','Apparel','Industrial','Grocery']),
        round(random.uniform(10,1000),2)
    ])

for i in range(1,21):
    suppliers.append([
        f'S{i:03d}',
        fake.company(),
        random.choice(['North','South','East','West']),
        random.randint(1,14)
    ])

for i,city in enumerate(['Delhi','Mumbai','Bengaluru','Hyderabad','Kolkata','Pune','Chennai','Jaipur'],1):
    warehouses.append([
        f'W{i:02d}',
        city,
        random.randint(10000,50000)
    ])


def write(name, header, rows):
    with open(ROOT/name,'w',newline='',encoding='utf-8') as f:
        w=csv.writer(f); w.writerow(header); w.writerows(rows)


write(
    'products.csv',
    ['product_id','product_name','category','unit_price'],
    products
)

write(
    'suppliers.csv',
    ['supplier_id','supplier_name','region','lead_time_days'],
    suppliers
)

write(
    'warehouses.csv',
    ['warehouse_id','city','capacity'],
    warehouses
)


base=datetime.now()-timedelta(days=30)

orders=[]; shipments=[]; inventory=[]

for i in range(1,5001):
    ts=base+timedelta(minutes=random.randint(0,30*24*60))
    pid=random.choice(products)[0]
    wid=random.choice(warehouses)[0]
    qty=random.randint(1,30)
    status=random.choice(['PLACED','PROCESSING','FULFILLED','CANCELLED'])

    orders.append([
        f'O{i:06d}',
        pid,
        wid,
        qty,
        status,
        ts.isoformat()
    ])


for i in range(1,4001):
    ts=base+timedelta(minutes=random.randint(0,30*24*60))
    order=f'O{random.randint(1,5000):06d}'
    carrier=random.choice(['DHL','FedEx','UPS','BlueDart','Delhivery'])
    promised=ts+timedelta(days=random.randint(1,7))
    delivered=promised+timedelta(days=random.choice([-1,0,0,1,2]))

    shipments.append([
        f'SHP{i:06d}',
        order,
        carrier,
        ts.isoformat(),
        promised.isoformat(),
        delivered.isoformat(),
        random.choice(['DELIVERED','IN_TRANSIT','DELAYED'])
    ])


for p in products:
    for w in warehouses:
        inventory.append([
            p[0],
            w[0],
            random.randint(0,1000),
            random.randint(100,300),
            datetime.now().isoformat()
        ])


write(
    'orders.csv',
    ['order_id','product_id','warehouse_id','quantity','status','order_timestamp'],
    orders
)

write(
    'shipments.csv',
    ['shipment_id','order_id','carrier','ship_timestamp','promised_delivery','actual_delivery','status'],
    shipments
)

write(
    'inventory.csv',
    ['product_id','warehouse_id','current_stock','reorder_level','updated_at'],
    inventory
)

print('Generated source data in', ROOT)
