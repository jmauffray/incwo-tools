

# --- Stage 2: Processing Functions (Same as before) ---
import glob
import os

import xml.etree.ElementTree as ET
import csv


def process_xml_to_csv(input_dir: str, output_file: str, fields: list):
    """Parses XML files and saves specific fields to a CSV file."""
    
    print(f"\n⚙️ Starting data processing and conversion to CSV...")
    
    search_path = os.path.join(input_dir, "customer_products_*.xml")
    xml_files = glob.glob(search_path)
    
    if not xml_files:
        print(f"⚠️ WARNING: No XML files found in '{input_dir}'. Processing skipped.")
        return False

    with open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        writer = csv.DictWriter(outfile, fieldnames=fields)
        writer.writeheader()
        
        product_count = 0
        
        for file_path in xml_files:
            print(f"- Read XMl file '{file_path}'.")
            try:
                tree = ET.parse(file_path)
                root = tree.getroot()
                
                for product in root.findall('customer_product'):
                    row_data = {}
                    for field_name in fields:
                        element = product.find(field_name)
                        value = element.text.strip() if element is not None and element.text else ""
                        row_data[field_name] = value.replace(',', ';')
                        
                    writer.writerow(row_data)
                    product_count += 1
                    
            except ET.ParseError as e:
                print(f"  [ERROR] Skipping file {file_path}: XML parsing error: {e}")

    print(f"\n✅ Processing complete. {product_count} records saved to: {output_file}")
    return True