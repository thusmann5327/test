

import requests
import sqlite3
import json
from datetime import datetime

class HarborAPI:
   def __init__(self):
       self.base_url = "https://api.harborwholesale.com/api"
       self.account_id = "700030"
       self.session = requests.Session()
       
   def authenticate(self, token):
       """Set up authentication for all requests with the provided token"""
       self.session.headers.update({
           'Authorization': f'Bearer {token}',
           'Content-Type': 'application/json'
       })

   def get_document_header(self, document_id):
       """Get invoice header details"""
       url = f"{self.base_url}/v2.0/OrderHistory/{self.account_id}/GetPostedDocumentHeader"
       params = {'documentId': document_id}
       response = self.session.get(url, params=params)
       response.raise_for_status()
       return response.json()
       
   def get_line_items(self, document_id):
       """Get line items for document"""
       url = f"{self.base_url}/v2.0/OrderHistory/{self.account_id}/GetPostedDocumentLines"
       params = {'documentId': document_id}
       data = {'documentId': document_id}
       response = self.session.post(url, params=params, json=data)
       response.raise_for_status()
       
       # Add debug logging
       print("\nLine Items Response:")
       print(json.dumps(response.json(), indent=2))
       
       return response.json()

   def get_categories(self, document_id):
       """Get categories for document"""
       url = f"{self.base_url}/v2.0/Category/{self.account_id}/GetCategoriesForPostedDocument"
       params = {'documentId': document_id}
       response = self.session.get(url, params=params)
       response.raise_for_status()
       return response.json()

   def get_items(self, item_ids):
       """Get item details"""
       if not item_ids:
           return {'Value': []}
           
       url = f"{self.base_url}/v1.0/Item/{self.account_id}/items"
       params = {'includeNonSellableUOMs': 'false'}
       
       # Create filter string for multiple items
       filter_str = " or ".join([f"ItemID eq '{id}'" for id in item_ids])
       data = {
           "Filter": f"({filter_str})",
           "Top": 100,  # Increased from 50 to handle more items
           "OrderBy": "ItemDescription asc"
       }
       
       response = self.session.post(url, params=params, json=data)
       response.raise_for_status()
       return response.json()

def drop_tables(conn):
   """Drop existing tables to recreate them"""
   cursor = conn.cursor()
   cursor.execute('DROP TABLE IF EXISTS harbor_invoice_items')
   cursor.execute('DROP TABLE IF EXISTS harbor_categories')
   cursor.execute('DROP TABLE IF EXISTS harbor_invoices')
   conn.commit()

def setup_database():
   """Create harbor_invoices and related tables if they don't exist"""
   conn = sqlite3.connect('clover.db')
   cursor = conn.cursor()
   
   # Create categories table
   cursor.execute('''
   CREATE TABLE IF NOT EXISTS harbor_categories (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       invoice_id TEXT,
       category_name TEXT,
       category_id TEXT,
       item_count INTEGER,
       total_cost REAL,
       FOREIGN KEY (invoice_id) REFERENCES harbor_invoices (document_id)
   )
   ''')
   
   # Create main invoices table
   cursor.execute('''
   CREATE TABLE IF NOT EXISTS harbor_invoices (
       document_id TEXT PRIMARY KEY,
       document_type TEXT,
       bill_to_id TEXT,
       bill_to_name TEXT,
       bill_to_address TEXT,
       bill_to_city TEXT,
       bill_to_state TEXT,
       bill_to_zip TEXT,
       order_id TEXT,
       posted_date TEXT,
       order_date TEXT,
       due_date TEXT,
       ship_to_name TEXT,
       ship_to_address TEXT,
       ship_to_city TEXT,
       ship_to_state TEXT,
       ship_to_zip TEXT,
       payment_terms TEXT,
       payment_method TEXT,
       transaction_type TEXT,
       allowances REAL,
       charges REAL,
       discounts REAL,
       sales_tax REAL,
       subtotal REAL,
       invoice_total REAL,
       categories TEXT,
       items TEXT,
       raw_data TEXT,
       created_at TEXT DEFAULT CURRENT_TIMESTAMP
   )
   ''')
   
   # Create line items table
   cursor.execute('''
   CREATE TABLE IF NOT EXISTS harbor_invoice_items (
       id INTEGER PRIMARY KEY AUTOINCREMENT,
       invoice_id TEXT,
       item_id TEXT,
       item_description TEXT,
       brand_name TEXT,
       category_id TEXT,
       unit_price REAL,
       net_price REAL,
       quantity INTEGER,
       uom TEXT,
       retail_upc TEXT,
       vendor_id TEXT,
       srp REAL,
       margin_pct REAL,
       package_description TEXT,
       line_total REAL,
       FOREIGN KEY (invoice_id) REFERENCES harbor_invoices (document_id)
   )
   ''')
   
   conn.commit()
   return conn

def save_categories(conn, document_id, categories_data):
   """Save category information to database"""
   cursor = conn.cursor()
   
   # Clear existing categories for this invoice
   cursor.execute('DELETE FROM harbor_categories WHERE invoice_id = ?', (document_id,))
   
   # Insert new categories
   for category_name, category_data in categories_data.items():
       cursor.execute('''
       INSERT INTO harbor_categories 
       (invoice_id, category_name, category_id, item_count, total_cost)
       VALUES (?, ?, ?, ?, ?)
       ''', (
           document_id,
           category_name,
           category_data.get('CategoryID'),
           category_data.get('Count', 0),
           category_data.get('Cost', 0.0)
       ))
   
   conn.commit()

def save_line_items(conn, document_id, line_items_data, items_data):
   """Save line items to separate table"""
   cursor = conn.cursor()
   
   # Clear existing items for this invoice
   cursor.execute('DELETE FROM harbor_invoice_items WHERE invoice_id = ?', (document_id,))
   
   # Create a lookup dictionary for item details
   item_details = {
       item['ItemId']: item 
       for item in items_data.get('Value', [])
   }
   
   # Process each line item
   for line_item in line_items_data.get('Value', []):
       item_id = line_item.get('Item', {}).get('ItemId')  # Updated path to ItemID
       item_detail = item_details.get(item_id, {})
       uom = item_detail.get('UOMs', [{}])[0] if item_detail else {}
       
       # Calculate line total
       quantity = line_item.get('Quantity', 0)
       unit_price = line_item.get('UnitPrice', 0)
       line_total = quantity * unit_price
       
       cursor.execute('''
       INSERT INTO harbor_invoice_items 
       (invoice_id, item_id, item_description, brand_name, category_id,
        unit_price, net_price, quantity, uom, retail_upc, vendor_id, srp,
        margin_pct, package_description, line_total)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ''', (
           document_id,
           item_id,
           item_detail.get('ItemDescription'),
           item_detail.get('BrandName'),
           item_detail.get('CategoryID'),
           line_item.get('UnitPrice'),
           line_item.get('NetPrice'),
           quantity,
           line_item.get('UnitOfMeasure'),
           item_detail.get('RetailUPC'),
           item_detail.get('VendorID'),
           uom.get('SRP'),
           uom.get('MarginPct'),
           uom.get('PackageDescription'),
           line_total
       ))
   
   conn.commit()

def save_invoice(conn, invoice_data):
   """Save invoice data to database"""
   cursor = conn.cursor()
   
   print("\n=== Debug Logging for save_invoice ===")
   
   try:
       # Try to get header from raw_data if not found at top level
       header = invoice_data['raw_data']['header']
       
       print("\nHeader data found:")
       print(json.dumps(header, indent=2))
       
       cursor.execute('''
       INSERT OR REPLACE INTO harbor_invoices 
       (document_id, document_type, bill_to_id, bill_to_name, bill_to_address,
        bill_to_city, bill_to_state, bill_to_zip, order_id, posted_date,
        order_date, due_date, ship_to_name, ship_to_address, ship_to_city,
        ship_to_state, ship_to_zip, payment_terms, payment_method,
        transaction_type, allowances, charges, discounts, sales_tax,
        subtotal, invoice_total, categories, items, raw_data)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ''', (
           header['DocumentId'],
           header['DocumentType'],
           header['BillToId'],
           header['BillToName'],
           header['BillToAddress'],
           header['BillToCity'],
           header['BillToState'],
           header['BillToZip'],
           header['OrderId'],
           header['PostedDate'],
           header['OrderDate'],
           header['DueDate'],
           header['ShipToName'],
           header['ShipToAddress'],
           header['ShipToCity'],
           header['ShipToState'],
           header['ShipToZip'],
           header['PaymentTerms'],
           header['PaymentMethod'],
           header['TransactionType'],
           float(header['Allowances']),
           float(header['Charges']),
           float(header['Discounts']),
           float(header['SalesTax']),
           float(header['SubTotal']),
           float(header['InvoiceTotal']),
           json.dumps(invoice_data.get('categories', {})),
           json.dumps(invoice_data.get('items', [])),
           json.dumps(invoice_data.get('raw_data', {}))
       ))
       
       # Save categories separately if they exist
       if 'categories' in invoice_data:
           save_categories(conn, header['DocumentId'], invoice_data['categories'])
       
       # Save line items and items details
       if 'raw_data' in invoice_data and 'line_items' in invoice_data['raw_data']:
           save_line_items(
               conn, 
               header['DocumentId'], 
               invoice_data['raw_data']['line_items'],
               invoice_data['items']
           )
       
       conn.commit()
       print("\nSuccessfully saved to database!")
       
   except Exception as e:
       print(f"\nERROR in save_invoice: {str(e)}")
       print("Error type:", type(e).__name__)
       import traceback
       print("\nFull traceback:")
       print(traceback.format_exc())
       raise

def check_database():
   """Function to verify database contents"""
   conn = sqlite3.connect('clover.db')
   cursor = conn.cursor()
   
   print("\n=== Checking Database Contents ===")
   
   try:
       # Check invoices
       cursor.execute("""
       SELECT document_id, document_type, bill_to_name, invoice_total 
       FROM harbor_invoices
       """)
       invoices = cursor.fetchall()
       print("\nInvoices found:", len(invoices))
       for inv in invoices:
           print(f"Invoice {inv[0]}: {inv[1]} - {inv[2]} - ${inv[3]:.2f}")
           
       # Check categories
       cursor.execute("""
       SELECT invoice_id, category_name, item_count, total_cost 
       FROM harbor_categories
       ORDER BY total_cost DESC
       """)
       categories = cursor.fetchall()
       print(f"\nCategories found:", len(categories))
       for cat in categories:
           print(f"Invoice {cat[0]}: {cat[1]} - {cat[2]} items - ${cat[3]:.2f}")
           
       # Check line items
       cursor.execute("""
       SELECT invoice_id, item_id, item_description, unit_price, srp, quantity, line_total 
       FROM harbor_invoice_items
       ORDER BY line_total DESC
       """)
       items = cursor.fetchall()
       print(f"\nLine items found:", len(items))
       for item in items:
           print(f"Invoice {item[0]}: {item[1]} - {item[2]} - ${item[3]:.2f} x {item[5]} (SRP: ${item[4]:.2f}) = ${item[6]:.2f}")
           
   except sqlite3.Error as e:
       print(f"Error checking database: {e}")
   finally:
       conn.close()

def main():
    # Initialize API client
    api = HarborAPI()
    
    # Your Bearer token (this should be obtained securely)
    token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik5UWkVSa1F3UlVSR09FRXdSRFpDUkVFM1FVWTNNVGN4TkVJMFFUWkJPREkwUkRsR05URkROUSJ9.eyJodHRwOi8vaGFyYm9yZm9vZHMvYXV0aG9yaXplZC1jb21wYW5pZXMiOlsiSFdGIl0sImh0dHA6Ly9oYXJib3J3aG9sZXNhbGUvbG9naW4tZW1haWwiOiJ0aG9tYXMuYS5odXNtYW5uQGdtYWlsLmNvbSIsImh0dHA6Ly9oYXJib3J3aG9sZXNhbGUvc2FsZXMtcmVwLWlkcyI6W10sImh0dHA6Ly9oYXJib3Jmb29kc2VydmljZS9zYWxlcy1yZXAtaWRzIjpbXSwiaHR0cDovL2hhcmJvcndob2xlc2FsZS9hdXRob3JpemVkLWFjY291bnRzIjpbIjcwMDAzMCJdLCJodHRwOi8vaGFyYm9yZm9vZHNlcnZpY2UvYXV0aG9yaXplZC1hY2NvdW50cyI6W10sImlzcyI6Imh0dHBzOi8vaGFyYm9yd2hvbGVzYWxlLmF1dGgwLmNvbS8iLCJzdWIiOiJhdXRoMHw2NDEwYTczY2E5YTRkM2MxYzk5M2IyOGMiLCJhdWQiOlsiaHR0cHM6Ly9hcGkuaGFyYm9yd2hvbGVzYWxlLmNvbSIsImh0dHBzOi8vaGFyYm9yd2hvbGVzYWxlLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE3MzM4ODI1MzAsImV4cCI6MTczMzk2ODkzMCwic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCBTSE9QUElOR0xJU1RTOlJFQUQgU0hPUFBJTkdMSVNUUzpTSE9QIFNIT1BQSU5HTElTVFM6Q09QWVRPTkVXIFNIT1BQSU5HTElTVFM6UkVTRVFMSVNUIFNIT1BQSU5HTElTVFM6Q1JFQVRFIFNIT1BQSU5HTElTVFM6VVBEQVRFIFNIT1BQSU5HTElTVFM6REVMRVRFIFNIT1BQSU5HTElTVFM6Q1JFQVRFTElORSBTSE9QUElOR0xJU1RTOlVQREFURUxJTkUgU0hPUFBJTkdMSVNUUzpERUxFVEVMSU5FIFNIT1BQSU5HTElTVFM6UkVQUklDRSBJVEVNUzpSRUFEIElURU1ISVNUT1JZOlJFQUQgQ1VTVE9NRVJTOlJFQUQgU0hPUFBJTkdDQVJUUzpSRUFEIFNIT1BQSU5HQ0FSVFM6TU9ESUZZIFNIT1BQSU5HQ0FSVFM6U1VCTUlUU0FMRVNPUkRFUiBTSE9QUElOR0NBUlRTOlNVQk1JVFRBR09SREVSIFNIT1BQSU5HQ0FSVFM6Q0hBTkdFQ0FSVFRZUEUgU0hPUFBJTkdDQVJUUzpSRVNFVFBST0dSRVNTIENBVEVHT1JJRVM6Q1JFQVRFIENBVEVHT1JJRVM6UkVBRCBDQVRFR09SSUVTOlVQREFURSBDQVRFR09SSUVTOkRFTEVURSBCTEFOS0VUT1JERVI6UkVBRCBSRVRVUk5PUkRFUjpSRUFEIFNBTEVTT1JERVI6UkVBRCBTQUxFU09SREVSOk1PRElGWSBQUklDRUlOUVVJUlk6UkVRVUVTVCBCUkFORFM6UkVBRCBUQUdTOlJFQUQgVVNFUkFDQ09VTlRTOlNDT1BFUyBJVEVNQVVUSDpSRUFEIElURU1BVVRIOlNFVFJVTEUgQ09NTUVOVFM6UkVBRCBPUkRFUkhJU1RPUlk6UkVBRCBSRVRBSUxQUklDSU5HOlJFQUQgUkVUQUlMUFJJQ0lORzpVUERBVEUgSVRFTUFVVEg6TU9ESUZZQk9PSyBCTEFOS0VUT1JERVI6TU9ESUZZIFVTRVJBQ0NPVU5UUzpVUERBVEUgSVRFTTpRVFlPTkhBTkQiLCJhenAiOiI2eDM3dlhaZTVrc2xHc3JFenl6TTMzcVhHaWt4Y2h3ZyJ9.PFkjQ0EEcxTSxsU1Ep0h80w9BgPUuWi3yYhU65aA5MKmZNljqUrjd3BBwQ0wHNfDrcdW9brBWjGLg_6wG0yz_6qjbda1P3K8dX3EYqjl04LTsqhaJgE3A426_RghP6FhSyedTCpbjf5KsSrkZdFwaBbiSZcByaB9GOzBOY8LQFjax03UgV4Md9VglWvMzFTFOrMJxmElO488C8R16Nep3fBW7LczIAVhMJidCGJrsjwgrxXstKJsP_YP7VgtJ4cP2qHqfacsxXS6zovshlnBvpPEb8AJB_fkzN8D0cdZ4D1UWL3NUOj5OVvbnCGmLEHzUqxtbiaLoR6wtKEmIsqG0g"
    # Set up database connection
    conn = sqlite3.connect('clover.db')
    
    try:
        # Drop and recreate tables
        print("Dropping existing tables...")
        drop_tables(conn)
        
        print("Creating new tables...")
        conn = setup_database()
        
        # Authenticate
        api.authenticate(token)
        
        # Example document ID
        document_id = "2349466"  # Updated to the invoice we were examining
        
        print(f"\nFetching document header for ID: {document_id}")
        header_data = api.get_document_header(document_id)
        
        print("\nFetching categories...")
        categories_data = api.get_categories(document_id)
        
        print("\nFetching line items...")
        line_items_data = api.get_line_items(document_id)
        
        print("\nFetching item details...")
        # Extract item IDs from line items with debug logging
        print("Line items data structure:", json.dumps(line_items_data, indent=2))
        
        # Safely extract item IDs
        try:
            item_ids = []
            for item in line_items_data.get('Value', []):
                item_id = item.get('Item', {}).get('ItemId')  # Try different path
                if item_id:
                    item_ids.append(item_id)
                else:
                    print(f"Warning: Could not find ItemID in item: {json.dumps(item, indent=2)}")
        except Exception as e:
            print(f"Error extracting item IDs: {str(e)}")
            print("Item structure:", json.dumps(line_items_data, indent=2))
            item_ids = []
            
        print(f"Found {len(item_ids)} item IDs: {item_ids}")
        items_data = api.get_items(item_ids) if item_ids else {'Value': []}
        
        # Prepare invoice data for storage
        invoice_data = {
            'document_id': document_id,
            'categories': categories_data,
            'items': items_data,
            'raw_data': {
                'header': header_data,
                'categories': categories_data,
                'items': items_data,
                'line_items': line_items_data
            }
        }
        
        # Save to database
        save_invoice(conn, invoice_data)
        
        print(f"Successfully processed invoice {document_id}")
        
        # Verify database contents
        check_database()
        
    except Exception as e:
        print(f"Error processing invoice: {str(e)}")
        import traceback
        print("\nFull traceback:")
        print(traceback.format_exc())
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()
   
   
   
   
   
   
   
   
   
   
   
   












