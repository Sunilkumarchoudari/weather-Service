import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class ExcelService:
    @staticmethod
    def generate_excel_report(db, hours=48):
        
        try:
            recent_data = db.get_recent_data(hours)
            
            if not recent_data:
                raise ValueError(f"No weather data available for the last {hours} hours")
            
            df = pd.DataFrame(recent_data)
            
            columns_to_export = ['timestamp', 'temperature_2m', 'relative_humidity_2m']
            df_export = df[columns_to_export].copy()
            
            df_export['timestamp'] = pd.to_datetime(df_export['timestamp'])
            df_export['temperature_2m'] = pd.to_numeric(df_export['temperature_2m'], errors='coerce')
            df_export['relative_humidity_2m'] = pd.to_numeric(df_export['relative_humidity_2m'], errors='coerce')
            
            df_export = df_export.sort_values('timestamp')
            
            output = BytesIO()
            
            with pd.ExcelWriter(output, engine='openpyxl', datetime_format='YYYY-MM-DD HH:MM:SS') as writer:
                df_export.to_excel(
                    writer, 
                    sheet_name='Weather Data', 
                    index=False,
                    float_format='%.2f'
                )
                
                workbook = writer.book
                worksheet = writer.sheets['Weather Data']
                
                from openpyxl.styles import Font, PatternFill, Alignment
                
                header_font = Font(bold=True, color='FFFFFF')
                header_fill = PatternFill(start_color='366092', end_color='366092', fill_type='solid')
                
                for cell in worksheet[1]: 
                    cell.font = header_font
                    cell.fill = header_fill
                    cell.alignment = Alignment(horizontal='center')
                
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    
                    adjusted_width = min(max_length + 2, 30)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            output.seek(0)
            logger.info(f"Generated Excel report with {len(df_export)} records")
            
            return output
            
        except Exception as e:
            logger.error(f"Error generating Excel report: {e}")
            raise

if __name__ == '__main__':
    print("Excel Service module loaded")
