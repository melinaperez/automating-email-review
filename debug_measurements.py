#!/usr/bin/env python3
"""
Script para debuggear específicamente las mediciones
"""

import os
import json
from datetime import datetime
from file_validator import FileValidator
from monitoring_system import MonitoringSystem
import logging

# Configurar logging detallado
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_measurements():
    """Debug específico de mediciones"""
    
    print("🔍 DEBUG DETALLADO DE MEDICIONES")
    print("=" * 60)
    
    # 1. Verificar archivos en data/
    data_dir = "data"
    if not os.path.exists(data_dir):
        print("❌ No existe directorio data/")
        return
    
    validator = FileValidator()
    
    # Recopilar todos los archivos por paciente
    patients_files = {}
    
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith(('.csv', '.pdf')):
                file_path = os.path.join(root, file)
                
                # Extraer nombre del paciente del path
                path_parts = root.split(os.sep)
                if len(path_parts) >= 2:
                    patient_folder = path_parts[1]  # data/PACIENTE_EMAIL/fecha/
                    
                    if patient_folder not in patients_files:
                        patients_files[patient_folder] = []
                    
                    patients_files[patient_folder].append(file_path)
    
    print(f"📁 Pacientes encontrados: {len(patients_files)}")
    
    # 2. Analizar cada paciente
    for patient_folder, files in patients_files.items():
        print(f"\n👤 PACIENTE: {patient_folder}")
        print(f"   Archivos totales: {len(files)}")
        
        valid_measurements = []
        
        for file_path in files:
            if file_path.endswith('.csv'):
                result = validator.validate_csv_file(file_path)
            else:
                result = validator.validate_pdf_file(file_path)
            
            if result['is_valid'] and result.get('measurement_time'):
                valid_measurements.append({
                    'file': file_path,
                    'time': result['measurement_time'],
                    'slot': result.get('time_slot', 'unknown'),
                    'type': 'pressure' if '.csv' in file_path else 'ecg'
                })
                print(f"   ✅ {os.path.basename(file_path)} -> {result['measurement_time']} ({result.get('time_slot', 'unknown')})")
            else:
                print(f"   ❌ {os.path.basename(file_path)} -> INVÁLIDO")
        
        print(f"   📊 Mediciones válidas: {len(valid_measurements)}")
        
        # Agrupar por fecha y franja
        grouped = {}
        for measurement in valid_measurements:
            time_obj = datetime.fromisoformat(measurement['time'].replace('Z', ''))
            date_key = time_obj.date().isoformat()
            slot = measurement['slot']
            
            if date_key not in grouped:
                grouped[date_key] = {}
            if slot not in grouped[date_key]:
                grouped[date_key][slot] = {'pressure': False, 'ecg': False}
            
            grouped[date_key][slot][measurement['type']] = True
        
        # Contar mediciones completas
        complete_measurements = 0
        for date_key, day_data in grouped.items():
            for slot, measurements in day_data.items():
                if measurements['pressure'] and measurements['ecg']:
                    complete_measurements += 1
                    print(f"   🎯 COMPLETA: {date_key} {slot}")
        
        print(f"   🏆 MEDICIONES COMPLETAS: {complete_measurements}")
    
    # 3. Ejecutar el sistema y ver qué pasa
    print(f"\n🔧 EJECUTANDO SISTEMA DE MONITOREO...")
    
    system = MonitoringSystem()
    
    # Simular procesamiento sin emails (solo archivos existentes)
    # Necesitamos cargar los datos existentes manualmente
    
    print(f"\n📋 RESULTADO DEL SISTEMA:")
    report = system.generate_completeness_report()
    
    for patient_name, patient_data in report['patients'].items():
        print(f"   👤 {patient_name}: {patient_data['received_measurements']}/{patient_data['expected_measurements']} ({patient_data['completion_percentage']}%)")

if __name__ == "__main__":
    debug_measurements()
