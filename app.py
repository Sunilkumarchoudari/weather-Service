from flask import Flask, jsonify, request, send_file
import logging
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.weather_service import WeatherService
from services.excel_service import ExcelService
from services.pdf_service import PDFService
from database import WeatherDatabase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

weather_service = WeatherService()
excel_service = ExcelService()
pdf_service = PDFService()
db = WeatherDatabase()

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({
        "message": "Weather Service API",
        "version": "2.0.0",
        "status": "operational",
        "features": [
            "Open-Meteo API integration",
            "SQLite data storage",
            "Excel report export",
            "PDF reports with charts"
        ],
        "endpoints": {
            "weather_report": "/weather-report?lat={lat}&lon={lon}",
            "export_excel": "/export/excel",
            "export_pdf": "/export/pdf",
            "data_summary": "/data/summary",
            "recent_data": "/data/recent?hours=24",
            "health": "/health"
        },
        "examples": [
            "curl 'http://localhost:5000/weather-report?lat=47.37&lon=8'",
            "curl -o weather_data.xlsx 'http://localhost:5000/export/excel'",
            "curl -o weather_report.pdf 'http://localhost:5000/export/pdf'"
        ],
        "timestamp": datetime.now().isoformat()
    })

@app.route('/weather-report', methods=['GET'])
def get_weather_report():
    try:
        lat = request.args.get('lat', type=float)
        lon = request.args.get('lon', type=float)
        
        if lat is None or lon is None:
            return jsonify({
                "error": "Missing parameters",
                "message": "Both 'lat' and 'lon' parameters required",
                "example": "/weather-report?lat=47.37&lon=8"
            }), 400
        
        if not (-90 <= lat <= 90):
            return jsonify({"error": "Invalid latitude (-90 to 90)"}), 400
        if not (-180 <= lon <= 180):
            return jsonify({"error": "Invalid longitude (-180 to 180)"}), 400
        
        logger.info(f"weather request for: {lat}, {lon}")
        
        result = weather_service.fetch_weather_data(lat, lon)
        
        if result.get("data"):
            stored = db.store_weather_data(result["data"])
            result["database_stored"] = stored
            logger.info(f"Stored {stored} records in database")
        else:
            result["database_stored"] = 0
            
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error processing weather report: {e}")
        return jsonify({
            "error": "Failed to fetch weather data",
            "message": str(e),
            "status": "failed"
        }), 500

@app.route('/export/excel', methods=['GET'])
def export_excel():
    try:
        hours = request.args.get('hours', default=48, type=int)
        
        if hours <= 0 or hours > 168:
            return jsonify({
                "error": "Invalid hours parameter (1-168)"
            }), 400
        
        logger.info(f"Generating Excel report for last {hours} hours")
        
        excel_file = excel_service.generate_excel_report(db, hours)
        
        return send_file(
            excel_file,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=f'weather_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        )
        
    except ValueError as e:
        return jsonify({
            "error": "No data available",
            "message": str(e)
        }), 404
    except Exception as e:
        logger.error(f"Error generating Excel report: {e}")
        return jsonify({
            "error": "Failed to generate Excel report",
            "message": str(e)
        }), 500

@app.route('/export/pdf', methods=['GET'])
def export_pdf():
    try:
        hours = request.args.get('hours', default=48, type=int)
        
        if hours <= 0 or hours > 168:
            return jsonify({
                "error": "Invalid hours parameter (1-168)"
            }), 400
        
        logger.info(f"Generating PDF report for last {hours} hours")
        
        pdf_file = pdf_service.generate_pdf_report(db, hours)
        
        return send_file(
            pdf_file,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=f'weather_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
        )
        
    except ValueError as e:
        return jsonify({
            "error": "No data available",
            "message": str(e)
        }), 404
    except Exception as e:
        logger.error(f"Error generating PDF report: {e}")
        return jsonify({
            "error": "Failed to generate PDF report",
            "message": str(e)
        }), 500

@app.route('/data/summary', methods=['GET'])
def get_data_summary():
    try:
        summary = db.get_data_summary()
        return jsonify(summary)
    except Exception as e:
        logger.error(f"Error getting summary: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/data/recent', methods=['GET'])
def get_recent_data():
    try:
        hours = request.args.get('hours', default=48, type=int)
        if hours <= 0 or hours > 168:
            return jsonify({
                "error": "Invalid hours parameter (1-168)"
            }), 400
            
        data = db.get_recent_data(hours)
        return jsonify({
            "hours_requested": hours,
            "records_found": len(data),
            "data": data[:10],
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error getting recent data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "weather_api": "operational",
            "database": "operational",
            "excel_export": "operational",
            "pdf_export": "operational"
        },
        "version": "1.0.0"
    })

if __name__ == '__main__':
    print("\nStarting Fask Weather Service...")
    print("API available at: http://localhost:5000")
    print("Weather data: curl 'http://localhost:5000/weather-report?lat=47.37&lon=8'")
    print("Excel export: curl -o weather_data.xlsx 'http://localhost:5000/export/excel'")
    print("PDF report: curl -o weather_report.pdf 'http://localhost:5000/export/pdf'")
    print("Health check: curl 'http://localhost:5000/health'\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
