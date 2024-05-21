from flask import Flask, request
import os
import zcatalyst_sdk
import csv
from io import StringIO

csvFormat = {
    "OrderId": 0,
    "ProductId": 1,
    "CustomerId": 2,
    "ProductName": 3,
    "Category": 4,
    "Region": 5,
    "DateOfSale": 6,
    "QuantitySold": 7,
    "UnitPrice": 8,
    "Discount": 9,
    "ShippingCost": 10,
    "PaymentMethod": 11,
    "Name": 12,
    "Email": 13,
    "Address": 14,
} 

app = Flask(__name__)

@app.route('/')
def index():
    return 'Web App with Python Flask!'

@app.route('/load')
def loadCSVtoCloud():
    fileId =  request.args.get('fileId')
    catalystApp = zcatalyst_sdk.initialize(req=request)
    db = catalystApp.datastore()
    zcql = catalystApp.zcql()
    fs = catalystApp.filestore()
    downloadedFile = fs.folder(1755000000020359).download_file(fileId).decode("utf-8")
    # with open(downloadedFile, newline='') as csvfile:
    reader = csv.reader(StringIO(downloadedFile), delimiter=',', quotechar='|')
    skipHeaders = True
    for row in reader:
        print(row)
        if skipHeaders:
            skipHeaders = False
            continue

        productId = row[csvFormat.get('ProductId')]            
        dbFetchedProduct = zcql.execute_query(f"select * from Product where ProductId = '{productId}'")
        dbProductId = dbFetchedProduct[0].get("Product").get("ROWID") if len(dbFetchedProduct) > 0 else "" 

        customerId = row[csvFormat.get('CustomerId')]            
        dbFetchedCustomer = zcql.execute_query(f"select * from Customer where CustomerId = '{customerId}'")
        dbCustomerId = dbFetchedCustomer[0].get("Customer").get("ROWID") if len(dbFetchedCustomer) > 0 else "" 


        if len(dbFetchedProduct)==0:
            prouductInsertResult = db.table('Product').insert_row({
                'ProductId': productId,
                'ProductName': row[csvFormat.get('ProductName')],
                'Category': row[csvFormat.get('Category')],
                'UnitPrice': float(row[csvFormat.get('UnitPrice')])
            })
            dbProductId = prouductInsertResult.get('ROWID')

        if len(dbFetchedCustomer)==0:
            customerInsertResult = db.table('Customer').insert_row({
                'CustomerId': customerId,
                'Name': row[csvFormat.get('Name')],
                'Email': row[csvFormat.get('Email')],
                'Address': row[csvFormat.get('Address')]
            })
            dbCustomerId = customerInsertResult.get('ROWID')

        try:
            db.table('Orders').insert_row({
                'CustomerId': dbCustomerId,
                'ProductId': dbProductId,
                'Region': row[csvFormat.get('Region')],
                'QuantitySold': int(row[csvFormat.get('QuantitySold')]),
                'Discount': float(row[csvFormat.get('Discount')]),
                'ShippingCost': float(row[csvFormat.get('ShippingCost')]),
                'PaymentMethod': row[csvFormat.get('PaymentMethod')],
                'DateOfSale': row[csvFormat.get('DateOfSale')]
            })
        except Exception as e: 
            print("Check the data, May be a duplicate order!")

    return 'Data Loaded Successfully'

@app.route('/revenue')
def revenue():
    fromDate = request.args.get('from')
    toDate = request.args.get('to')
    catalystApp = zcatalyst_sdk.initialize(req=request)
    zcql = catalystApp.zcql()
    result = zcql.execute_query(f"select * from Orders where DateOfSale between '{fromDate}' and '{toDate}'")
    revenue = 0
    for order in result:
        pId = order.get('Orders').get('ProductId')
        unitPrice = zcql.execute_query(f"select * from Product where ROWID = '{pId}'")[0].get('Product').get('UnitPrice')
        discount = (float(unitPrice) * int(order.get('Orders').get('QuantitySold'))) * float(order.get('Orders').get('Discount'))
        revenue += (float(unitPrice) * int(order.get('Orders').get('QuantitySold'))) - discount
    return f' The revenue is {revenue}'


@app.route('/revenueByProduct')
def revenueByProduct():
    fromDate = request.args.get('from')
    toDate = request.args.get('to')
    productIdFromReq = request.args.get('product')
    catalystApp = zcatalyst_sdk.initialize(req=request)
    zcql = catalystApp.zcql()
    result = zcql.execute_query(f"select * from Orders where DateOfSale between '{fromDate}' and '{toDate}'")
    revenue = 0
    for order in result:
        pId = order.get('Orders').get('ProductId')
        product = zcql.execute_query(f"select * from Product where ROWID = '{pId}'")
        if str(product[0].get('Product').get('ProductId')).lower() != productIdFromReq.lower():
            continue
        unitPrice = product[0].get('Product').get('UnitPrice')
        discount = (float(unitPrice) * int(order.get('Orders').get('QuantitySold'))) * float(order.get('Orders').get('Discount'))
        revenue += (float(unitPrice) * int(order.get('Orders').get('QuantitySold'))) - discount
    return f' The revenue is {revenue}'

@app.route('/revenueByCategory')
def revenueByCategory():
    fromDate = request.args.get('from')
    toDate = request.args.get('to')
    categoryFromReq = request.args.get('category')
    catalystApp = zcatalyst_sdk.initialize(req=request)
    zcql = catalystApp.zcql()
    result = zcql.execute_query(f"select * from Orders where DateOfSale between '{fromDate}' and '{toDate}'")
    revenue = 0
    for order in result:
        pId = order.get('Orders').get('ProductId')
        product = zcql.execute_query(f"select * from Product where ROWID = '{pId}'")
        if str(product[0].get('Product').get('Category')).lower() != categoryFromReq.lower():
            continue
        unitPrice = product[0].get('Product').get('UnitPrice')
        discount = (float(unitPrice) * int(order.get('Orders').get('QuantitySold'))) * float(order.get('Orders').get('Discount'))
        revenue += (float(unitPrice) * int(order.get('Orders').get('QuantitySold'))) - discount
    return f' The revenue is {revenue}'


@app.route('/revenueByRegion')
def revenueByRegion():
    fromDate = request.args.get('from')
    toDate = request.args.get('to')
    region = request.args.get('region')
    catalystApp = zcatalyst_sdk.initialize(req=request)
    zcql = catalystApp.zcql()
    result = zcql.execute_query(f"select * from Orders where Region = {region} and DateOfSale between '{fromDate}' and '{toDate}'")
    revenue = 0
    for order in result:
        pId = order.get('Orders').get('ProductId')
        product = zcql.execute_query(f"select * from Product where ROWID = '{pId}'")
        unitPrice = product[0].get('Product').get('UnitPrice')
        discount = (float(unitPrice) * int(order.get('Orders').get('QuantitySold'))) * float(order.get('Orders').get('Discount'))
        revenue += (float(unitPrice) * int(order.get('Orders').get('QuantitySold'))) - discount
    return f' The revenue is {revenue}'


listen_port = os.getenv('X_ZOHO_CATALYST_LISTEN_PORT', 9000)
app.run(host='0.0.0.0', port = listen_port)
