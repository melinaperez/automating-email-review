#!/usr/bin/env python3
"""
Script de diagnóstico detallado para entender exactamente qué archivos hay y cómo se están procesando
"""

import os
import re
import pandas as pd
from datetime import datetime
from file_validator import FileValidator
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def diagnose_files_detailed():
    """Diagnóstico detallado de archivos"""
    
    data_dir = "data"
    if not os.path.exists(data_dir):
        print("❌ No existe el directorio 'data'")
        return
    
    validator = FileValidator()
    
    print("\n🔍 DIAGNÓSTICO DETALLADO DE ARCHIVOS")
    print("=" * 50)
    
    # Estructura para almacenar información por paciente
    patient_files = {}
    
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
                    if patient_name not in patient_files:
                        patient_files[patient_name] = []
                    
                    # Validar archivo
                    if file.endswith('.csv'):
                        result = validator.validate_csv_file(file_path)
                        file_type = 'pressure'
                    else:
                        result = validator.validate_pdf_file(file_path)
                        file_type = 'ecg'
                    
                    # Guardar información del archivo
                    patient_files[patient_name].append({
                        'file_path': file_path,
                        'file_name': file,
                        'file_type': file_type,
                        'is_valid': result.get('is_valid', False),
                        'measurement_time': result.get('measurement_time'),
                        'time_slot': result.get('time_slot'),
                        'errors': result.get('errors', []),
                        'warnings': result.get('warnings', [])
                    })
    
    # Mostrar información detallada por paciente
    for patient_name, files in patient_files.items():
        print(f"\n👤 {patient_name}")
        print(f"   Total archivos: {len(files)}")
        
        # Contar archivos por tipo
        pressure_files = [f for f in files if f['file_type'] == 'pressure' and f['is_valid']]
        ecg_files = [f for f in files if f['file_type'] == 'ecg' and f['is_valid']]
        
        print(f"   Archivos de presión válidos: {len(pressure_files)}")
        print(f"   Archivos de ECG válidos: {len(ecg_files)}")
        
        # Organizar por fecha y franja horaria
        organized_files = {}
        
        for file_info in files:
            if not file_info['is_valid'] or not file_info['measurement_time']:
                continue
                
            # Convertir a datetime si es string
            if isinstance(file_info['measurement_time'], str):
                try:
                    measurement_time = datetime.fromisoformat(file_info['measurement_time'].replace('Z', '').replace('+00:00', ''))
                except ValueError:
                    print(f"   ❌ Error parseando fecha: {file_info['measurement_time']}")
                    continue
            else:
                measurement_time = file_info['measurement_time']
            
            date_key = measurement_time.date().isoformat()
            time_slot = file_info['time_slot'] or 'unknown'
            
            # Inicializar estructura
            if date_key not in organized_files:
                organized_files[date_key] = {}
            
            if time_slot not in organized_files[date_key]:
                organized_files[date_key][time_slot] = {'pressure': None, 'ecg': None}
            
            # Registrar archivo
            organized_files[date_key][time_slot][file_info['file_type']] = file_info['file_path']
        
        # Mostrar archivos organizados
        print("\n   📅 Archivos por fecha y franja horaria:")
        
        complete_measurements = 0
        
        for date, slots in sorted(organized_files.items()):
            print(f"\n   📆 {date}:")
            
            for slot_name, files in sorted(slots.items()):
                pressure = "✅" if files['pressure'] else "❌"
                ecg = "✅" if files['ecg'] else "❌"
                
                # Verificar si esta medición está completa
                is_complete = files['pressure'] and files['ecg']
                if is_complete:
                    complete_measurements += 1
                    status = "✅ COMPLETA"
                else:
                    status = "❌ INCOMPLETA"
                
                print(f"      {slot_name}: Presión {pressure} | ECG {ecg} | {status}")
                
                # Mostrar rutas de archivos
                if files['pressure']:
                    print(f"         Presión: {os.path.basename(files['pressure'])}")
                if files['ecg']:
                    print(f"         ECG: {os.path.basename(files['ecg'])}")
        
        print(f"\n   📊 Mediciones completas (presión + ECG): {complete_measurements}")
    
    # Verificar estructura de directorios
    print("\n📁 ESTRUCTURA DE DIRECTORIOS:")
    patient_dirs = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    print(f"   Directorios de pacientes: {len(patient_dirs)}")
    
    for patient_dir in patient_dirs:
        print(f"   - {patient_dir}")
        
        # Verificar subdirectorios
        patient_path = os.path.join(data_dir, patient_dir)
        subdirs = [d for d in os.listdir(patient_path) if os.path.isdir(os.path.join(patient_path, d))]
        
        if subdirs:
            print(f"     Subdirectorios: {len(subdirs)}")
            for subdir in subdirs[:5]:  # Mostrar solo los primeros 5
                print(f"     - {subdir}")
            if len(subdirs) > 5:
                print(f"     ... y {len(subdirs) - 5} más")

if __name__ == "__main__":
    diagnose_files_detailed()
