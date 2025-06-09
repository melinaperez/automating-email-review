#!/usr/bin/env python3
"""
Analizador de archivos descargados - Segunda etapa del proceso
Solo se encarga de validar y analizar archivos ya descargados
"""

import os
import json
import logging
from improved_pressure_analyzer import ImprovedPressureAnalyzer
from datetime import datetime
from improved_file_validator import FileValidator
from improved_csv_processor import ImprovedCSVProcessor

logger = logging.getLogger(__name__)

class FileAnalyzer:
    def __init__(self):
        """Inicializa el analizador de archivos"""
        self.data_dir = "data"
        self.file_validator = FileValidator()
        self.csv_processor = ImprovedCSVProcessor()
        self.analysis_summary = {
            'patients_analyzed': 0,
            'csv_files_processed': 0,
            'pdf_files_processed': 0,
            'total_measurements': 0,
            'errors': [],
            'warnings': [],
            'patient_results': {}
        }
    
    def analyze_all_downloaded_files(self) -> dict:
        """
        Analiza todos los archivos descargados en el directorio data/
        
        Returns:
            Diccionario con resultados del análisis
        """
        logger.info("🔍 INICIANDO ANÁLISIS DE ARCHIVOS DESCARGADOS")
        logger.info("=" * 60)
        
        if not os.path.exists(self.data_dir):
            error_msg = f"Directorio '{self.data_dir}' no encontrado. Ejecute primero email_downloader.py"
            logger.error(error_msg)
            self.analysis_summary['errors'].append(error_msg)
            return self.analysis_summary
        
        # Obtener lista de pacientes
        patient_dirs = [d for d in os.listdir(self.data_dir) 
                       if os.path.isdir(os.path.join(self.data_dir, d))]
        
        if not patient_dirs:
            error_msg = "No se encontraron directorios de pacientes"
            logger.error(error_msg)
            self.analysis_summary['errors'].append(error_msg)
            return self.analysis_summary
        
        logger.info(f"👥 Pacientes encontrados: {len(patient_dirs)}")
        
        # Analizar cada paciente
        for patient_dir in patient_dirs:
            logger.info(f"\n{'='*60}")
            logger.info(f"👤 ANALIZANDO PACIENTE: {patient_dir}")
            logger.info(f"{'='*60}")
            
            try:
                patient_result = self.analyze_patient_files(patient_dir)
                self.analysis_summary['patient_results'][patient_dir] = patient_result
                self.analysis_summary['patients_analyzed'] += 1
                
            except Exception as e:
                error_msg = f"Error analizando paciente {patient_dir}: {str(e)}"
                logger.error(error_msg)
                self.analysis_summary['errors'].append(error_msg)
        
        # Mostrar resumen final
        self.show_analysis_summary()
        
        # Guardar resultados
        self.save_analysis_results()
        
        return self.analysis_summary
    
    def analyze_patient_files(self, patient_dir: str) -> dict:
        """
        Analiza todos los archivos de un paciente específico
        
        Args:
            patient_dir: Nombre del directorio del paciente
        
        Returns:
            Diccionario con resultados del análisis del paciente
        """
        patient_path = os.path.join(self.data_dir, patient_dir)
        
        patient_result = {
            'patient_name': patient_dir,
            'csv_analysis': {},
            'pdf_analysis': [],
            'pressure_measurements': {},
            'ecg_measurements': [],
            'completeness': {},
            'errors': [],
            'warnings': []
        }
        
        # Listar archivos del paciente
        files = os.listdir(patient_path)
        csv_files = [f for f in files if f.lower().endswith('.csv') or 'pressure' in f.lower()]
        pdf_files = [f for f in files if f.lower().endswith('.pdf') or 'ecg' in f.lower()]
        
        logger.info(f"📁 Archivos encontrados:")
        logger.info(f"   📊 CSV (presión): {len(csv_files)}")
        logger.info(f"   📄 PDF (ECG): {len(pdf_files)}")
        
        # ANÁLISIS DE ARCHIVOS CSV (PRESIÓN)
        if csv_files:
            logger.info(f"\n📊 ANALIZANDO ARCHIVOS CSV DE PRESIÓN...")
            
            # Usar el procesador mejorado para encontrar el mejor CSV
            best_csv = self.csv_processor.find_best_csv_file(patient_dir)
            
            if best_csv:
                logger.info(f"🏆 Procesando mejor archivo CSV: {os.path.basename(best_csv)}")
                
                # Extraer todas las mediciones del mejor CSV
                measurements = self.csv_processor.extract_all_pressure_measurements(best_csv)
                
                if measurements:
                    # Organizar mediciones por día y franja
                    organized_data = self.organize_measurements_by_day(measurements)
                    patient_result['pressure_measurements'] = organized_data
                    
                    # Calcular completitud
                    completeness = self.calculate_pressure_completeness(organized_data)
                    patient_result['completeness']['pressure'] = completeness
                    
                    self.analysis_summary['total_measurements'] += len(measurements)
                    logger.info(f"✅ {len(measurements)} mediciones de presión extraídas")
                else:
                    warning_msg = f"No se pudieron extraer mediciones de {best_csv}"
                    logger.warning(warning_msg)
                    patient_result['warnings'].append(warning_msg)
                
                patient_result['csv_analysis'] = {
                    'best_file': os.path.basename(best_csv),
                    'total_measurements': len(measurements) if measurements else 0,
                    'ignored_files': [f for f in csv_files if os.path.join(patient_path, f) != best_csv]
                }
                
                self.analysis_summary['csv_files_processed'] += 1
            else:
                error_msg = f"No se pudo encontrar archivo CSV válido para {patient_dir}"
                logger.error(error_msg)
                patient_result['errors'].append(error_msg)
        
        # ANÁLISIS DE ARCHIVOS PDF (ECG)
        if pdf_files:
            logger.info(f"\n📄 ANALIZANDO ARCHIVOS PDF DE ECG...")
            
            for pdf_file in pdf_files:
                pdf_path = os.path.join(patient_path, pdf_file)
                logger.info(f"   📄 Procesando: {pdf_file}")
                
                try:
                    # Validar archivo PDF
                    pdf_result = self.file_validator.validate_pdf_file(pdf_path)
                    
                    if pdf_result['is_valid']:
                        ecg_data = {
                            'file_name': pdf_file,
                            'measurement_time': pdf_result.get('measurement_time'),
                            'time_slot': pdf_result.get('time_slot'),
                            'patient_name': pdf_result.get('patient_name'),
                            'has_am_pm_ambiguity': pdf_result.get('has_am_pm_ambiguity', False),
                            'warnings': pdf_result.get('warnings', [])
                        }
                        
                        patient_result['ecg_measurements'].append(ecg_data)
                        logger.info(f"      ✅ ECG válido - {ecg_data['time_slot']} - {ecg_data['measurement_time']}")
                        
                        if ecg_data['has_am_pm_ambiguity']:
                            logger.warning(f"      ⚠️ Ambigüedad AM/PM detectada")
                    else:
                        error_msg = f"PDF inválido {pdf_file}: {pdf_result.get('errors', [])}"
                        logger.error(f"      ❌ {error_msg}")
                        patient_result['errors'].append(error_msg)
                    
                    patient_result['pdf_analysis'].append(pdf_result)
                    self.analysis_summary['pdf_files_processed'] += 1
                    
                except Exception as e:
                    error_msg = f"Error procesando PDF {pdf_file}: {str(e)}"
                    logger.error(f"      ❌ {error_msg}")
                    patient_result['errors'].append(error_msg)
        
        # ANÁLISIS DE COMPLETITUD GENERAL
        self.analyze_patient_completeness(patient_result)
        
        return patient_result
    
    def organize_measurements_by_day(self, measurements: list) -> dict:
        """Organiza las mediciones por día y franja horaria"""
        organized = {}
        
        for measurement in measurements:
            try:
                measurement_time = datetime.fromisoformat(measurement['measurement_time'])
                date_key = measurement_time.date().isoformat()
                time_slot = measurement['time_slot']
                
                if date_key not in organized:
                    organized[date_key] = {
                        'matutina': [],
                        'vespertina': []
                    }
                
                if time_slot in organized[date_key]:
                    organized[date_key][time_slot].append(measurement)
                    
            except Exception as e:
                logger.warning(f"Error organizando medición: {e}")
                continue
        
        return organized
    
    def calculate_pressure_completeness(self, organized_data: dict) -> dict:
        """Calcula la completitud de las mediciones de presión"""
        total_days = len(organized_data)
        complete_days = 0
        incomplete_days = []
        
        for date_key, day_data in organized_data.items():
            matutinas = len(day_data['matutina'])
            vespertinas = len(day_data['vespertina'])
            
            if matutinas >= 2 and vespertinas >= 2:
                complete_days += 1
            else:
                incomplete_days.append({
                    'date': date_key,
                    'matutinas': matutinas,
                    'vespertinas': vespertinas,
                    'missing': {
                        'matutinas': max(0, 2 - matutinas),
                        'vespertinas': max(0, 2 - vespertinas)
                    }
                })
        
        completeness_percentage = (complete_days / total_days * 100) if total_days > 0 else 0
        
        return {
            'total_days': total_days,
            'complete_days': complete_days,
            'incomplete_days': incomplete_days,
            'completeness_percentage': completeness_percentage
        }
    
    def analyze_patient_completeness(self, patient_result: dict):
        """Analiza la completitud general del paciente"""
        pressure_data = patient_result.get('pressure_measurements', {})
        ecg_data = patient_result.get('ecg_measurements', [])
        
        # Resumen de completitud
        completeness_summary = {
            'pressure_days': len(pressure_data),
            'ecg_count': len(ecg_data),
            'status': 'incomplete'
        }
        
        # Determinar estado general
        if pressure_data and ecg_data:
            pressure_completeness = patient_result.get('completeness', {}).get('pressure', {})
            if pressure_completeness.get('completeness_percentage', 0) >= 80:
                completeness_summary['status'] = 'complete'
            else:
                completeness_summary['status'] = 'partial'
        
        patient_result['completeness']['overall'] = completeness_summary
        
        # Log del estado
        status_emoji = {
            'complete': '✅',
            'partial': '⚠️',
            'incomplete': '❌'
        }
        
        emoji = status_emoji.get(completeness_summary['status'], '❓')
        logger.info(f"\n{emoji} ESTADO DEL PACIENTE: {completeness_summary['status'].upper()}")
        logger.info(f"   📊 Días con datos de presión: {completeness_summary['pressure_days']}")
        logger.info(f"   📄 Archivos ECG: {completeness_summary['ecg_count']}")
    
    def show_analysis_summary(self):
        """Muestra un resumen detallado del análisis"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 RESUMEN DE ANÁLISIS DE ARCHIVOS")
        logger.info("=" * 60)
        logger.info(f"👥 Pacientes analizados: {self.analysis_summary['patients_analyzed']}")
        logger.info(f"📊 Archivos CSV procesados: {self.analysis_summary['csv_files_processed']}")
        logger.info(f"📄 Archivos PDF procesados: {self.analysis_summary['pdf_files_processed']}")
        logger.info(f"📈 Total mediciones extraídas: {self.analysis_summary['total_measurements']}")
        logger.info(f"❌ Errores: {len(self.analysis_summary['errors'])}")
        logger.info(f"⚠️ Advertencias: {len(self.analysis_summary['warnings'])}")
        
        # Resumen por paciente
        if self.analysis_summary['patient_results']:
            logger.info(f"\n👥 RESUMEN POR PACIENTE:")
            
            for patient_name, result in self.analysis_summary['patient_results'].items():
                overall_status = result.get('completeness', {}).get('overall', {})
                status = overall_status.get('status', 'unknown')
                pressure_days = overall_status.get('pressure_days', 0)
                ecg_count = overall_status.get('ecg_count', 0)
                
                status_emoji = {'complete': '✅', 'partial': '⚠️', 'incomplete': '❌'}.get(status, '❓')
                
                logger.info(f"   {status_emoji} {patient_name}: {pressure_days} días presión, {ecg_count} ECGs")
    
    def save_analysis_results(self):
        """Guarda los resultados del análisis en archivos"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Guardar resumen completo en JSON
            results_filename = f"logs/analysis_results_{timestamp}.json"
            os.makedirs("logs", exist_ok=True)
            
            with open(results_filename, 'w', encoding='utf-8') as f:
                json.dump(self.analysis_summary, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📄 Resultados guardados en: {results_filename}")
            
            # Crear reporte resumido
            report_filename = f"logs/analysis_report_{timestamp}.txt"
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write("REPORTE DE ANÁLISIS DE ARCHIVOS MÉDICOS\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Pacientes analizados: {self.analysis_summary['patients_analyzed']}\n")
                f.write(f"Archivos CSV procesados: {self.analysis_summary['csv_files_processed']}\n")
                f.write(f"Archivos PDF procesados: {self.analysis_summary['pdf_files_processed']}\n")
                f.write(f"Total mediciones: {self.analysis_summary['total_measurements']}\n\n")
                
                for patient_name, result in self.analysis_summary['patient_results'].items():
                    f.write(f"PACIENTE: {patient_name}\n")
                    f.write("-" * 30 + "\n")
                    
                    # CSV info
                    csv_info = result.get('csv_analysis', {})
                    if csv_info:
                        f.write(f"Mejor CSV: {csv_info.get('best_file', 'N/A')}\n")
                        f.write(f"Mediciones: {csv_info.get('total_measurements', 0)}\n")
                    
                    # Completitud
                    completeness = result.get('completeness', {}).get('pressure', {})
                    if completeness:
                        f.write(f"Días completos: {completeness.get('complete_days', 0)}/{completeness.get('total_days', 0)}\n")
                        f.write(f"Completitud: {completeness.get('completeness_percentage', 0):.1f}%\n")
                    
                    f.write("\n")
            
            logger.info(f"📄 Reporte resumido en: {report_filename}")
            
        except Exception as e:
            logger.error(f"Error guardando resultados: {e}")

def main():
    """Función principal para ejecutar solo el análisis"""
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    print("🔍 ANALIZADOR DE ARCHIVOS MÉDICOS")
    print("=" * 50)
    print("Esta es la SEGUNDA ETAPA del proceso:")
    print("1. ❌ NO descargar emails (eso es la primera etapa)")
    print("2. ✅ Analizar archivos ya descargados")
    print("3. ✅ Validar archivos CSV y PDF")
    print("4. ✅ Extraer mediciones y clasificar")
    print("5. ✅ Calcular completitud")
    print()
    
    try:
        analyzer = FileAnalyzer()
        
        # Ejecutar análisis
        results = analyzer.analyze_all_downloaded_files()
        
        print(f"\n✅ ANÁLISIS COMPLETADO")
        print(f"👥 Pacientes analizados: {results['patients_analyzed']}")
        print(f"📊 Archivos CSV: {results['csv_files_processed']}")
        print(f"📄 Archivos PDF: {results['pdf_files_processed']}")
        print(f"📈 Mediciones: {results['total_measurements']}")
        
        if results['errors']:
            print(f"❌ Errores: {len(results['errors'])}")
        
        print("\n📄 Revise los archivos de log para detalles completos")
        
    except Exception as e:
        logger.error(f"Error en análisis: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
