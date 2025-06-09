#!/usr/bin/env python3
"""
Sistema de Monitoreo Médico Mejorado
Integra el ImprovedPressureAnalyzer para análisis correcto de CSV
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from email_reader import EmailReader
from improved_pressure_analyzer import ImprovedPressureAnalyzer
from file_validator import FileValidator

logger = logging.getLogger(__name__)

class MonitoringSystem:
    def __init__(self, config_file: str = "config.json"):
        """Inicializa el sistema de monitoreo con analizador mejorado"""
        self.config_file = config_file
        self.config = self.load_config()
        
        # Inicializar componentes
        self.email_reader = EmailReader(config_file)
        self.pressure_analyzer = ImprovedPressureAnalyzer()  # ¡NUEVO!
        self.file_validator = FileValidator()
        
        # Configurar directorios
        self.data_dir = "data"
        self.reports_path = "reports"
        self.logs_path = "logs"
        
        # Crear directorios si no existen
        for directory in [self.data_dir, self.reports_path, self.logs_path]:
            os.makedirs(directory, exist_ok=True)
        
        # Configurar logging
        self.setup_logging()
    
    def load_config(self) -> dict:
        """Carga la configuración del sistema"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error cargando configuración: {e}")
            return {}
    
    def setup_logging(self):
        """Configura el sistema de logging"""
        log_file = os.path.join(self.logs_path, f'monitoring_{datetime.now().strftime("%Y%m%d")}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def run_daily_check(self) -> dict:
        """
        Ejecuta el chequeo diario completo usando el analizador mejorado
        
        Returns:
            Diccionario con resumen del chequeo
        """
        logger.info("🏥 INICIANDO CHEQUEO DIARIO CON ANALIZADOR MEJORADO")
        logger.info("=" * 60)
        
        summary = {
            'timestamp': datetime.now().isoformat(),
            'emails_processed': 0,
            'files_validated': 0,
            'patients_processed': 0,
            'errors': [],
            'warnings': [],
            'patients_data': {}
        }
        
        try:
            # 1. ETAPA: Descargar emails (si es necesario)
            logger.info("📧 Etapa 1: Verificando emails...")
            try:
                email_summary = self.email_reader.download_all_attachments()
            except AttributeError:
                # Fallback si el método no existe
                email_summary = {'emails_processed': 0, 'files_downloaded': 0}
            summary['emails_processed'] = email_summary.get('emails_processed', 0)
            
            # 2. ETAPA: Analizar archivos con analizador mejorado
            logger.info("🔍 Etapa 2: Analizando archivos con analizador mejorado...")
            patients_data = self.analyze_all_patients()
            summary['patients_data'] = patients_data
            summary['patients_processed'] = len(patients_data)
            
            # 3. ETAPA: Generar reporte
            logger.info("📊 Etapa 3: Generando reporte...")
            report_data = self.generate_monitoring_report(patients_data)
            
            # 4. Guardar reporte
            report_file = self.save_report(report_data)
            logger.info(f"📄 Reporte guardado en: {report_file}")
            
            logger.info("✅ Chequeo diario completado exitosamente")
            
        except Exception as e:
            error_msg = f"Error en chequeo diario: {str(e)}"
            logger.error(error_msg)
            summary['errors'].append(error_msg)
        
        return summary
    
    def analyze_all_patients(self) -> dict:
        """
        Analiza todos los pacientes usando el analizador mejorado de presión
        
        Returns:
            Diccionario con datos de todos los pacientes
        """
        patients_data = {}
        
        if not os.path.exists(self.data_dir):
            logger.warning(f"Directorio {self.data_dir} no encontrado")
            return patients_data
        
        # Obtener lista de pacientes
        patient_dirs = [d for d in os.listdir(self.data_dir) 
                       if os.path.isdir(os.path.join(self.data_dir, d))]
        
        logger.info(f"👥 Analizando {len(patient_dirs)} pacientes...")
        
        for patient_dir in patient_dirs:
            logger.info(f"\n🏥 Procesando paciente: {patient_dir}")
            
            try:
                # Analizar datos de presión con el analizador mejorado
                pressure_data = self.pressure_analyzer.process_patient_pressure_data(patient_dir)
                
                # Analizar archivos ECG
                ecg_data = self.analyze_patient_ecg_files(patient_dir)
                
                # Calcular completitud
                completeness = self.calculate_patient_completeness(pressure_data, ecg_data)
                
                # Almacenar datos del paciente
                patients_data[patient_dir] = {
                    'pressure_data': pressure_data,
                    'ecg_data': ecg_data,
                    'completeness': completeness,
                    'last_updated': datetime.now().isoformat()
                }
                
                logger.info(f"✅ {patient_dir}: {completeness['completeness_percentage']:.1f}% completo")
                
            except Exception as e:
                error_msg = f"Error procesando {patient_dir}: {str(e)}"
                logger.error(error_msg)
                continue
        
        return patients_data
    
    def analyze_patient_ecg_files(self, patient_dir: str) -> list:
        """Analiza los archivos ECG de un paciente"""
        ecg_data = []
        patient_path = os.path.join(self.data_dir, patient_dir)
        
        if not os.path.exists(patient_path):
            return ecg_data
        
        # Buscar archivos PDF (ECG)
        files = os.listdir(patient_path)
        pdf_files = [f for f in files if f.lower().endswith('.pdf')]
        
        for pdf_file in pdf_files:
            pdf_path = os.path.join(patient_path, pdf_file)
            
            try:
                # Validar archivo PDF
                pdf_result = self.file_validator.validate_pdf_file(pdf_path)
                
                if pdf_result.get('is_valid', False):
                    ecg_entry = {
                        'file_name': pdf_file,
                        'measurement_time': pdf_result.get('measurement_time'),
                        'time_slot': pdf_result.get('time_slot'),
                        'patient_name': pdf_result.get('patient_name'),
                        'warnings': pdf_result.get('warnings', [])
                    }
                    ecg_data.append(ecg_entry)
                    
            except Exception as e:
                logger.warning(f"Error procesando ECG {pdf_file}: {e}")
                continue
        
        return ecg_data
    
    def calculate_patient_completeness(self, pressure_data: dict, ecg_data: list) -> dict:
        """
        Calcula la completitud del paciente basado en datos reales
        
        Args:
            pressure_data: Datos de presión organizados por día y franja
            ecg_data: Lista de archivos ECG válidos
        
        Returns:
            Diccionario con información de completitud
        """
        total_days = len(pressure_data)
        complete_days = 0
        incomplete_days = []
        
        # Analizar cada día
        for date_key, day_data in pressure_data.items():
            matutinas_pressure = len(day_data.get('matutina', []))
            vespertinas_pressure = len(day_data.get('vespertina', []))
            
            # Contar ECGs para este día
            date_obj = datetime.fromisoformat(date_key).date()
            day_ecgs = []
            
            for ecg in ecg_data:
                if ecg.get('measurement_time'):
                    try:
                        ecg_date = datetime.fromisoformat(ecg['measurement_time']).date()
                        if ecg_date == date_obj:
                            day_ecgs.append(ecg)
                    except:
                        continue
            
            # Clasificar ECGs por franja
            matutinas_ecg = len([e for e in day_ecgs if e.get('time_slot') == 'matutina'])
            vespertinas_ecg = len([e for e in day_ecgs if e.get('time_slot') == 'vespertina'])
            
            # Verificar completitud (2 presiones + 2 ECGs por franja)
            matutina_complete = matutinas_pressure >= 2 and matutinas_ecg >= 2
            vespertina_complete = vespertinas_pressure >= 2 and vespertinas_ecg >= 2
            
            day_complete = matutina_complete and vespertina_complete
            
            if day_complete:
                complete_days += 1
            else:
                incomplete_days.append({
                    'date': date_key,
                    'matutina': {
                        'pressure': matutinas_pressure,
                        'ecg': matutinas_ecg,
                        'complete': matutina_complete
                    },
                    'vespertina': {
                        'pressure': vespertinas_pressure,
                        'ecg': vespertinas_ecg,
                        'complete': vespertina_complete
                    }
                })
        
        # Calcular porcentaje de completitud
        completeness_percentage = (complete_days / total_days * 100) if total_days > 0 else 0
        
        return {
            'total_days': total_days,
            'complete_days': complete_days,
            'incomplete_days': incomplete_days,
            'completeness_percentage': completeness_percentage,
            'is_complete': completeness_percentage >= 80,
            'requirements': {
                'pressure_per_slot': 2,
                'ecg_per_slot': 2
            }
        }
    
    def generate_monitoring_report(self, patients_data: dict) -> dict:
        """
        Genera el reporte de monitoreo con datos reales
        
        Args:
            patients_data: Datos de todos los pacientes
        
        Returns:
            Diccionario con el reporte completo
        """
        # Calcular estadísticas generales
        total_patients = len(patients_data)
        patients_complete = sum(1 for p in patients_data.values() 
                              if p['completeness']['is_complete'])
        patients_incomplete = total_patients - patients_complete
        
        # Preparar datos de pacientes para el reporte
        patients_report = {}
        
        for patient_name, patient_data in patients_data.items():
            pressure_data = patient_data['pressure_data']
            completeness = patient_data['completeness']
            
            # Organizar datos diarios para el reporte
            daily_data = {}
            total_measurements_received = 0
            total_measurements_expected = 0
            
            for date_key, day_data in pressure_data.items():
                daily_data[date_key] = {
                    'matutina': {
                        'pressure_count': len(day_data.get('matutina', [])),
                        'pressure_data': [
                            {
                                'time': datetime.fromisoformat(m['measurement_time']).strftime('%H:%M'),
                                'systolic': m['data'].get('systolic'),
                                'diastolic': m['data'].get('diastolic'),
                                'pulse': m['data'].get('pulse')
                            }
                            for m in day_data.get('matutina', [])
                        ],
                        'pressure': [  # NUEVO: Formato compatible con dashboard
                            {
                                'time': datetime.fromisoformat(m['measurement_time']).strftime('%H:%M'),
                                'systolic': m['data'].get('systolic'),
                                'diastolic': m['data'].get('diastolic'),
                                'pulse': m['data'].get('pulse')
                            }
                            for m in day_data.get('matutina', [])
                        ],
                        'ecg': []  # NUEVO: Placeholder para ECGs
                    },
                    'vespertina': {
                        'pressure_count': len(day_data.get('vespertina', [])),
                        'pressure_data': [
                            {
                                'time': datetime.fromisoformat(m['measurement_time']).strftime('%H:%M'),
                                'systolic': m['data'].get('systolic'),
                                'diastolic': m['data'].get('diastolic'),
                                'pulse': m['data'].get('pulse')
                            }
                            for m in day_data.get('vespertina', [])
                        ],
                        'pressure': [  # NUEVO: Formato compatible con dashboard
                            {
                                'time': datetime.fromisoformat(m['measurement_time']).strftime('%H:%M'),
                                'systolic': m['data'].get('systolic'),
                                'diastolic': m['data'].get('diastolic'),
                                'pulse': m['data'].get('pulse')
                            }
                            for m in day_data.get('vespertina', [])
                        ],
                        'ecg': []  # NUEVO: Placeholder para ECGs
                    }
                }
                
                # Contar mediciones para estadísticas
                matutina_complete = len(day_data.get('matutina', [])) >= 2
                vespertina_complete = len(day_data.get('vespertina', [])) >= 2
                
                if matutina_complete:
                    total_measurements_received += 1
                if vespertina_complete:
                    total_measurements_received += 1
                
                # No incrementar aquí, se calculará al final
            
            # ESTÁNDAR: 14 franjas esperadas (2 franjas × 7 días)
            standard_expected = 14
            
            patients_report[patient_name] = {
                'completion_percentage': completeness['completeness_percentage'],
                'is_complete': completeness['is_complete'],
                'daily_data': daily_data,
                'requirements': completeness['requirements'],
                'received_measurements': total_measurements_received,
                'expected_measurements': standard_expected,
                'missing_measurements': []  # NUEVO: Para compatibilidad
            }
        
        # Calcular totales generales con estándar de 14 franjas por paciente
        total_measurements_received_all = sum(p['received_measurements'] for p in patients_report.values())
        total_measurements_expected_all = len(patients_report) * 14  # 14 franjas por paciente
        
        # Crear reporte final
        report = {
            'generation_date': datetime.now().isoformat(),
            'overall_summary': {
                'total_patients': total_patients,
                'patients_complete': patients_complete,
                'patients_incomplete': patients_incomplete,
                'total_measurements_received': total_measurements_received_all,
                'total_measurements_expected': total_measurements_expected_all
            },
            'patients': patients_report
        }
        
        return report
    
    def save_report(self, report_data: dict) -> str:
        """Guarda el reporte en archivo JSON"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"monitoring_report_{timestamp}.json"
        report_path = os.path.join(self.reports_path, report_filename)
        
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False, default=str)
            
            return report_path
            
        except Exception as e:
            logger.error(f"Error guardando reporte: {e}")
            return ""

def main():
    """Función principal para pruebas"""
    system = MonitoringSystem()
    summary = system.run_daily_check()
    
    print(f"\n✅ Chequeo completado:")
    print(f"👥 Pacientes procesados: {summary['patients_processed']}")
    print(f"📧 Emails procesados: {summary['emails_processed']}")
    
    if summary['errors']:
        print(f"❌ Errores: {len(summary['errors'])}")

if __name__ == "__main__":
    main()
