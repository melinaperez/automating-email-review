#!/usr/bin/env python3
"""
Script para corregir el sistema de monitoreo
"""

import os
import json
import logging
from datetime import datetime
from file_validator import FileValidator

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_monitoring_system():
    """Corrige el sistema de monitoreo"""
    
    print("\n🔧 CORRECCIÓN DEL SISTEMA DE MONITOREO")
    print("=" * 50)
    
    data_dir = "data"
    if not os.path.exists(data_dir):
        print("❌ No existe el directorio 'data'")
        return
    
    validator = FileValidator()
    
    # Estructura para almacenar información por paciente
    patient_data = {}
    
    # Recorrer todos los directorios y archivos
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith(('.csv', '.pdf')):
                file_path = os.path.join(root, file)
                
                # Extraer nombre del paciente de la ruta
                path_parts = root.split(os.sep)
                if len(path_parts) >= 2:
                    patient_folder = path_parts[1]  # data/PATIENT_FOLDER/...
                    
                    # Extraer nombre del paciente (antes del guion bajo si hay email)
                    patient_name = patient_folder.split('_')[0] if '_' in patient_folder else patient_folder
                    
                    # Inicializar estructura si no existe
                    if patient_name not in patient_data:
                        patient_data[patient_name] = {
                            'measurements': [],
                            'last_update': datetime.now().isoformat()
                        }
                    
                    # Validar archivo
                    if file.endswith('.csv'):
                        result = validator.validate_csv_file(file_path)
                        file_type = 'pressure'
                    else:
                        result = validator.validate_pdf_file(file_path)
                        file_type = 'ecg'
                    
                    # Solo procesar archivos válidos
                    if result.get('is_valid', False):
                        # Crear entrada de medición
                        measurement = {
                            'timestamp': datetime.now().isoformat(),
                            'patient_name': patient_name,
                            'file_path': file_path,
                            'is_valid': True,
                            'errors': [],
                            'warnings': result.get('warnings', []),
                            'measurement_time': result.get('measurement_time'),
                            'time_slot': result.get('time_slot'),
                            'file_type': file_type  # Añadir tipo de archivo explícitamente
                        }
                        
                        # Agregar a la lista de mediciones del paciente
                        patient_data[patient_name]['measurements'].append(measurement)
    
    # Generar reporte de completitud
    report = generate_completeness_report(patient_data)
    
    # Guardar reporte
    reports_dir = "reports"
    os.makedirs(reports_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(reports_dir, f"fixed_report_{timestamp}.json")
    
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n✅ Reporte corregido guardado en: {report_path}")
    
    # Mostrar resumen
    print("\n📊 RESUMEN DEL REPORTE CORREGIDO:")
    print(f"   Total pacientes: {report['overall_summary']['total_patients']}")
    print(f"   Pacientes completos: {report['overall_summary']['patients_complete']}")
    print(f"   Pacientes incompletos: {report['overall_summary']['patients_incomplete']}")
    print(f"   Mediciones esperadas: {report['overall_summary']['total_measurements_expected']}")
    print(f"   Mediciones recibidas: {report['overall_summary']['total_measurements_received']}")
    
    # Mostrar detalles por paciente
    print("\n👥 DETALLES POR PACIENTE:")
    for patient_name, patient_info in report['patients'].items():
        print(f"   👤 {patient_name}: {patient_info['received_measurements']}/{patient_info['expected_measurements']} ({patient_info['completion_percentage']}%)")

def generate_completeness_report(patient_data):
    """Genera un reporte de completitud para todos los pacientes"""
    report = {
        'generation_date': datetime.now().isoformat(),
        'patients': {},
        'overall_summary': {
            'total_patients': 0,
            'patients_complete': 0,
            'patients_incomplete': 0,
            'total_measurements_expected': 0,
            'total_measurements_received': 0
        }
    }
    
    # Valores predeterminados
    study_days = 7
    measurements_per_day = 4
    
    for patient_name, patient_info in patient_data.items():
        patient_report = analyze_patient_completeness(
            patient_name, 
            patient_info, 
            study_days, 
            measurements_per_day
        )
        
        report['patients'][patient_name] = patient_report
        
        # Actualizar resumen general
        report['overall_summary']['total_patients'] += 1
        if patient_report['is_complete']:
            report['overall_summary']['patients_complete'] += 1
        else:
            report['overall_summary']['patients_incomplete'] += 1
        
        report['overall_summary']['total_measurements_expected'] += patient_report['expected_measurements']
        report['overall_summary']['total_measurements_received'] += patient_report['received_measurements']
    
    return report

def analyze_patient_completeness(patient_name, patient_info, study_days, measurements_per_day):
    """Analiza la completitud de un paciente específico"""
    measurements = patient_info['measurements']
    
    # Organizar mediciones por día y franja horaria
    daily_data = {}
    
    for measurement in measurements:
        # Solo procesar mediciones válidas
        if not measurement.get('is_valid', False):
            continue
        
        # Obtener tiempo de medición
        measurement_time_str = measurement.get('measurement_time')
        if not measurement_time_str:
            continue
        
        try:
            # Parsear el tiempo de medición
            if isinstance(measurement_time_str, str):
                # Remover 'Z' y '+00:00' si están presentes
                measurement_time_str = measurement_time_str.replace('Z', '').replace('+00:00', '')
                measurement_time = datetime.fromisoformat(measurement_time_str)
            else:
                measurement_time = measurement_time_str
            
            date_key = measurement_time.date().isoformat()
            time_slot = measurement.get('time_slot', 'unknown')
            
            # Inicializar estructura de datos diarios si no existe
            if date_key not in daily_data:
                daily_data[date_key] = {
                    'matutina_1': {'pressure': False, 'ecg': False},
                    'matutina_2': {'pressure': False, 'ecg': False},
                    'vespertina_1': {'pressure': False, 'ecg': False},
                    'vespertina_2': {'pressure': False, 'ecg': False}
                }
            
            # Determinar tipo de archivo
            file_type = measurement.get('file_type')
            if not file_type:
                file_path = measurement.get('file_path', '')
                if 'pressure' in file_path.lower() or '.csv' in file_path.lower():
                    file_type = 'pressure'
                elif 'ecg' in file_path.lower() or '.pdf' in file_path.lower():
                    file_type = 'ecg'
                else:
                    continue
            
            # Marcar la medición como recibida si la franja horaria es válida
            if time_slot in daily_data[date_key]:
                daily_data[date_key][time_slot][file_type] = True
                
        except Exception as e:
            print(f"Error procesando medición: {e}")
            continue
    
    # Calcular completitud basado en TODAS las mediciones encontradas
    received_measurements = 0
    missing_measurements = []
    
    # Contar mediciones completas en TODAS las fechas encontradas
    for date_key, day_data in daily_data.items():
        for time_slot, measurements_slot in day_data.items():
            if measurements_slot['pressure'] and measurements_slot['ecg']:
                received_measurements += 1
            else:
                missing_items = []
                if not measurements_slot['pressure']:
                    missing_items.append('presión')
                if not measurements_slot['ecg']:
                    missing_items.append('ECG')
                
                if missing_items:  # Solo agregar si realmente faltan elementos
                    missing_measurements.append({
                        'date': date_key,
                        'time_slot': time_slot,
                        'missing': missing_items
                    })
    
    # Calcular mediciones esperadas basado en el período de estudio configurado
    expected_measurements = study_days * measurements_per_day
    
    # Si tenemos más mediciones de las esperadas, ajustar las esperadas
    if received_measurements > expected_measurements:
        expected_measurements = received_measurements
    
    completion_percentage = (received_measurements / expected_measurements * 100) if expected_measurements > 0 else 0
    
    return {
        'patient_name': patient_name,
        'expected_measurements': expected_measurements,
        'received_measurements': received_measurements,
        'completion_percentage': round(completion_percentage, 2),
        'is_complete': completion_percentage >= 100,
        'missing_measurements': missing_measurements,
        'daily_data': daily_data,
        'last_update': patient_info['last_update']
    }

if __name__ == "__main__":
    fix_monitoring_system()
