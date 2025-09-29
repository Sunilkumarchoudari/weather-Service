import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
import base64
import logging
from weasyprint import HTML, CSS
import os

logger = logging.getLogger(__name__)

class PDFService:
    @staticmethod
    def generate_pdf_report(db, hours=48):
        
        try:
            recent_data = db.get_recent_data(hours)
            
            if not recent_data:
                raise ValueError(f"No weather data available for the last {hours} hours")
            
            
            df = pd.DataFrame(recent_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
            
            chart_base64 = PDFService._create_weather_chart(df)
            
            metadata = {
                'location': f"Lat: {df['latitude'].iloc[0]:.4f}, Lon: {df['longitude'].iloc[0]:.4f}",
                'date_range': f"{df['timestamp'].min().strftime('%Y-%m-%d %H:%M')} to {df['timestamp'].max().strftime('%Y-%m-%d %H:%M')}",
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_records': len(df),
                'hours_covered': hours
            }
            
            stats = {
                'temperature': {
                    'avg': df['temperature_2m'].mean(),
                    'max': df['temperature_2m'].max(),
                    'min': df['temperature_2m'].min()
                },
                'humidity': {
                    'avg': df['relative_humidity_2m'].mean(),
                    'max': df['relative_humidity_2m'].max(),
                    'min': df['relative_humidity_2m'].min()
                }
            }
            
            html_content = PDFService._generate_html_content(metadata, stats, chart_base64)
            
            output = BytesIO()
            HTML(string=html_content).write_pdf(output)
            output.seek(0)
            
            logger.info(f"Generated PDF report with {len(df)} records")
            return output
            
        except Exception as e:
            logger.error(f"Error generating PDF report: {e}")
            raise
    
    @staticmethod
    def _create_weather_chart(df):
        
        try:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
            
            ax1.plot(df['timestamp'], df['temperature_2m'], 'r-', linewidth=2, marker='o', markersize=3)
            ax1.set_ylabel('Temperature (°C)', fontsize=12, color='red')
            ax1.set_title('Weather Data - Temperature and Humidity Trends', fontsize=14, fontweight='bold')
            ax1.grid(True, alpha=0.3)
            ax1.tick_params(axis='y', labelcolor='red')
            
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            ax1.xaxis.set_major_locator(mdates.HourLocator(interval=6))
            
            ax2.plot(df['timestamp'], df['relative_humidity_2m'], 'b-', linewidth=2, marker='s', markersize=3)
            ax2.set_ylabel('Relative Humidity (%)', fontsize=12, color='blue')
            ax2.set_xlabel('Time', fontsize=12)
            ax2.grid(True, alpha=0.3)
            ax2.tick_params(axis='y', labelcolor='blue')
            
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            ax2.xaxis.set_major_locator(mdates.HourLocator(interval=6))
            
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            
            plt.tight_layout()
            
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight', facecolor='white')
            buffer.seek(0)
            chart_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            plt.close(fig)
            
            return chart_base64
            
        except Exception as e:
            logger.error(f"Error creating chart: {e}")
            return ""
    
    @staticmethod
    def _generate_html_content(metadata, stats, chart_base64):
               
        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Weather Data Report</title>
            <style>
                body {{ 
                    font-family: 'Arial', sans-serif; 
                    margin: 40px; 
                    color: #333;
                    line-height: 1.6;
                }}
                .header {{ 
                    text-align: center; 
                    border-bottom: 3px solid #366092;
                    padding-bottom: 20px;
                    margin-bottom: 30px;
                }}
                h1 {{ 
                    color: #366092; 
                    font-size: 28px;
                    margin-bottom: 10px;
                }}
                .subtitle {{
                    color: #666;
                    font-size: 16px;
                    font-style: italic;
                }}
                .metadata {{ 
                    background-color: #f8f9fa; 
                    padding: 20px; 
                    border-radius: 8px; 
                    margin: 20px 0;
                    border-left: 4px solid #366092;
                }}
                .metadata h3 {{
                    color: #366092;
                    margin-top: 0;
                }}
                .chart {{ 
                    text-align: center; 
                    margin: 30px 0;
                    page-break-inside: avoid;
                }}
                .chart h3 {{
                    color: #366092;
                    margin-bottom: 15px;
                }}
                .statistics {{ 
                    margin: 30px 0; 
                }}
                .statistics h3 {{
                    color: #366092;
                    border-bottom: 2px solid #366092;
                    padding-bottom: 5px;
                }}
                table {{ 
                    width: 100%; 
                    border-collapse: collapse; 
                    margin: 20px 0;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                th, td {{ 
                    border: 1px solid #ddd; 
                    padding: 12px; 
                    text-align: center;
                }}
                th {{ 
                    background-color: #366092; 
                    color: white;
                    font-weight: bold;
                }}
                tr:nth-child(even) {{
                    background-color: #f9f9f9;
                }}
                .footer {{
                    margin-top: 40px;
                    padding-top: 20px;
                    border-top: 1px solid #ddd;
                    text-align: center;
                    color: #666;
                    font-size: 12px;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Weather Data Report</h1>
                <div class="subtitle">Weather Analysis Report</div>
            </div>
            
            <div class="metadata">
                <h3>Report Information</h3>
                <p><strong>Location:</strong> {metadata['location']}</p>
                <p><strong>Date Range:</strong> {metadata['date_range']}</p>
                <p><strong>Data Period:</strong> Last {metadata['hours_covered']} hours</p>
                <p><strong>Generated:</strong> {metadata['generated_at']}</p>
                <p><strong>Total Records:</strong> {metadata['total_records']}</p>
            </div>
            
            <div class="chart">
                <h3>Temperature and Humidity Trends</h3>
                {f'<img src="data:image/png;base64,{chart_base64}" style="max-width: 100%; height: auto;">' if chart_base64 else '<p>Chart could not be generated</p>'}
            </div>
            
            <div class="statistics">
                <h3>Statistical Summary</h3>
                <table>
                    <tr>
                        <th>Metric</th>
                        <th>Temperature (°C)</th>
                        <th>Humidity (%)</th>
                    </tr>
                    <tr>
                        <td><strong>Average</strong></td>
                        <td>{stats['temperature']['avg']:.2f}</td>
                        <td>{stats['humidity']['avg']:.1f}</td>
                    </tr>
                    <tr>
                        <td><strong>Maximum</strong></td>
                        <td>{stats['temperature']['max']:.2f}</td>
                        <td>{stats['humidity']['max']:.1f}</td>
                    </tr>
                    <tr>
                        <td><strong>Minimum</strong></td>
                        <td>{stats['temperature']['min']:.2f}</td>
                        <td>{stats['humidity']['min']:.1f}</td>
                    </tr>
                </table>
            </div>
            
            <div class="footer">
                <p>Generated by Weather Service API | Data provided by Open-Meteo</p>
            </div>
        </body>
        </html>
        """
        
        return html_template

if __name__ == '__main__':
    print("PDF Service module loaded")
