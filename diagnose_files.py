#!/usr/bin/env python3
"""
Script de diagnóstico para verificar archivos y su procesamiento
"""

import os
import json
from datetime import datetime
from file_validator import FileValidator
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def diagnose_files():
    """Diagnostica todos los archivos en el directorio data"""
    
    data_dir = "data"
    if not os.path.exists(data_dir):
        print("❌ No existe el directorio 'data'")
        return
    
    validator = FileValidator()
    
    print("🔍 DIAGNÓSTICO DE ARCHIVOS")
    print("=" * 50)
    
    total_files = 0
    valid_files = 0
    files_with_time = 0
    
    # Recorrer todos los subdirectorios
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith(('.csv', '.pdf')):
                total_files += 1
                file_path = os.path.join(root, file)
                
                print(f"\n📁 {file_path}")
                
                # Validar archivo
                if file.endswith('.csv'):
                    result = validator.validate_csv_file(file_path)
                else:
                    result = validator.validate_pdf_file(file_path)
                
                print(f"   ✅ Válido: {result['is_valid']}")
                
                if result['is_valid']:
                    valid_files += 1
                
                if result.get('measurement_time'):
                    files_with_time += 1
                    print(f"   🕐 Tiempo: {result['measurement_time']}")
                    print(f"   📊 Franja: {result.get('time_slot', 'No determinada')}")
                else:
                    print(f"   ❌ Sin tiempo de medición")
                
                if result.get('errors'):
                    print(f"   🚨 Errores: {result['errors']}")
                
                if result.get('warnings'):
                    print(f"   ⚠️  Advertencias: {result['warnings']}")
    
    print(f"\n📊 RESUMEN:")
    print(f"   Total archivos: {total_files}")
    print(f"   Archivos válidos: {valid_files}")
    print(f"   Archivos con tiempo: {files_with_time}")
    
    # Verificar reportes existentes
    reports_dir = "reports"
    if os.path.exists(reports_dir):
        report_files = [f for f in os.listdir(reports_dir) if f.endswith('.json')]
        print(f"   Reportes generados: {len(report_files)}")
        
        if report_files:
            latest_report = sorted(report_files)[-1]
            print(f"   Último reporte: {latest_report}")
            
            # Mostrar contenido del último reporte
            with open(os.path.join(reports_dir, latest_report), 'r', encoding='utf-8') as f:
                report = json.load(f)
            
            print(f"\n📋 CONTENIDO DEL ÚLTIMO REPORTE:")
            for patient_name, patient_data in report.get('patients', {}).items():
                print(f"   👤 {patient_name}: {patient_data['received_measurements']}/{patient_data['expected_measurements']} ({patient_data['completion_percentage']}%)")

if __name__ == "__main__":
    diagnose_files()
