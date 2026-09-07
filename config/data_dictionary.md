# Data Dictionary

## Orders
- order_id: unique order identifier
- product_id: product key
- warehouse_id: fulfillment warehouse
- quantity: units ordered
- status: order lifecycle status
- order_timestamp: event time

## Inventory
- current_stock: available units
- reorder_level: threshold for replenishment
- inventory_status: NORMAL / REORDER / CRITICAL

## Shipments
- shipment_id: shipment identifier
- carrier: logistics provider
- promised_delivery: expected arrival
- actual_delivery: observed arrival
- delay_days: days after promise, floored at zero
