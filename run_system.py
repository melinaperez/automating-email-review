#!/usr/bin/env python3
"""
Script principal para ejecutar el sistema de monitoreo médico
Puede ejecutarse manualmente o programarse como tarea automática
"""

import sys
import os
import logging
import argparse
from datetime import datetime
from monitoring_system import MonitoringSystem

def setup_logging(log_level='INFO'):
    """Configura el sistema de logging mejorado"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Crear directorio de logs si no existe
    os.makedirs('logs', exist_ok=True)
    
    # Nombre de archivo de log con timestamp
    log_filename = f'logs/monitoring_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    # Limpiar handlers existentes
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Configurar logging a archivo y consola
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )
    
    # Silenciar logs excesivos de pdfminer
    logging.getLogger('pdfminer').setLevel(logging.WARNING)
    logging.getLogger('pdfminer.psparser').setLevel(logging.ERROR)
    logging.getLogger('pdfminer.pdfdocument').setLevel(logging.ERROR)
    logging.getLogger('pdfminer.pdfinterp').setLevel(logging.ERROR)
    logging.getLogger('pdfminer.pdfpage').setLevel(logging.ERROR)
    logging.getLogger('pdfminer.converter').setLevel(logging.ERROR)
    
    # Configurar logger específico para debugging AM/PM
    ampm_logger = logging.getLogger('ampm_resolution')
    ampm_handler = logging.FileHandler(f'logs/ampm_resolution_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8')
    ampm_handler.setFormatter(logging.Formatter(log_format))
    ampm_logger.addHandler(ampm_handler)
    ampm_logger.setLevel(logging.DEBUG)
    
    print(f"Logs guardándose en: {log_filename}")
    return log_filename

def run_download_stage(config_file, force_all=False, log_level='INFO'):
    """Ejecuta la etapa de descarga de adjuntos"""
    logger = logging.getLogger(__name__)
    
    print("\n" + "="*60)
    print("🔽 ETAPA 1: DESCARGA DE ADJUNTOS")
    print("="*60)
    
    try:
        from email_downloader import EmailDownloader
        
        downloader = EmailDownloader(config_file)
        download_summary = downloader.download_all_attachments(force_all=force_all)
        
        print(f"\n📧 Emails procesados: {download_summary['emails_processed']}")
        print(f"📎 Adjuntos descargados: {download_summary.get('files_downloaded', 0)}")
        print(f"👥 Pacientes encontrados: {len(download_summary.get('patients', {}))}")
        
        if download_summary.get('patients'):
            print("\n👥 PACIENTES PROCESADOS:")
            for patient, count in download_summary['patients'].items():
                print(f"   • {patient}: {count} archivos")
        
        if download_summary.get('errors'):
            print(f"\n❌ Errores en descarga: {len(download_summary['errors'])}")
            for error in download_summary['errors'][:5]:  # Mostrar solo los primeros 5
                print(f"   • {error}")
        
        logger.info(f"Etapa de descarga completada: {download_summary.get('files_downloaded', 0)} archivos descargados")
        return download_summary
        
    except Exception as e:
        logger.error(f"Error en etapa de descarga: {e}", exc_info=True)
        print(f"❌ Error en descarga: {e}")
        return {'emails_processed': 0, 'files_downloaded': 0, 'patients': {}, 'errors': [str(e)]}

def run_analysis_stage(config_file, log_level='INFO'):
    """Ejecuta la etapa de análisis de archivos"""
    logger = logging.getLogger(__name__)
    
    print("\n" + "="*60)
    print("🔍 ETAPA 2: ANÁLISIS DE ARCHIVOS")
    print("="*60)
    
    try:
        from file_analyzer import FileAnalyzer
        
        analyzer = FileAnalyzer()  # Sin parámetros
        analysis_summary = analyzer.analyze_all_downloaded_files()
        
        print(f"\n📁 Archivos analizados: {analysis_summary.get('total_measurements', 0)}")
        print(f"📊 Archivos CSV procesados: {analysis_summary.get('csv_files_processed', 0)}")
        print(f"📄 Archivos PDF procesados: {analysis_summary.get('pdf_files_processed', 0)}")
        print(f"👥 Pacientes analizados: {analysis_summary.get('patients_analyzed', 0)}")
        
        if analysis_summary.get('patient_results'):
            print("\n📊 RESUMEN POR PACIENTE:")
            for patient, data in analysis_summary['patient_results'].items():
                completeness = data.get('completeness', {})
                total_days = completeness.get('total_days', 0)
                complete_days = completeness.get('complete_days', 0)
                percentage = (complete_days / total_days * 100) if total_days > 0 else 0
                
                print(f"   • {patient}:")
                print(f"     - Días completos: {complete_days}/{total_days} ({percentage:.1f}%)")
                print(f"     - CSV seleccionado: {data.get('selected_csv', 'N/A')}")
        
        if analysis_summary.get('errors'):
            print(f"\n❌ Errores en análisis: {len(analysis_summary['errors'])}")
            for error in analysis_summary['errors'][:5]:  # Mostrar solo los primeros 5
                print(f"   • {error}")
        
        # Generar reportes
        if 'completeness_report' in analysis_summary:
            analyzer.save_report(analysis_summary['completeness_report'])
            analyzer.export_to_excel(analysis_summary['completeness_report'])
            print("\n📋 Reportes generados en directorio 'reports/'")
        
        logger.info(f"Etapa de análisis completada: {analysis_summary.get('total_measurements', 0)} archivos analizados")
        return analysis_summary
        
    except Exception as e:
        logger.error(f"Error en etapa de análisis: {e}", exc_info=True)
        print(f"❌ Error en análisis: {e}")
        return {'total_measurements': 0, 'csv_files_processed': 0, 'pdf_files_processed': 0, 'patients_analyzed': 0, 'patient_results': {}, 'errors': [str(e)]}

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description='Sistema de Monitoreo Médico')
    parser.add_argument('--mode', choices=['check', 'dashboard', 'report', 'download', 'analyze'], 
                       default='check', help='Modo de ejecución')
    parser.add_argument('--config', default='config.json', 
                       help='Archivo de configuración')
    parser.add_argument('--log-level', default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Nivel de logging')
    parser.add_argument('--days-back', type=int, default=30,
                       help='Días hacia atrás para procesar emails')
    parser.add_argument('--force-all', action='store_true',
                       help='Procesar todos los emails, incluso los ya leídos')
    
    args = parser.parse_args()
    
    # Configurar logging
    log_filename = setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info(f"Iniciando sistema de monitoreo médico en modo: {args.mode}")
    logger.info(f"Logs guardándose en: {log_filename}")
    
    try:
        if args.mode == 'check':
            # Modo de chequeo completo: descarga + análisis
            logger.info("Ejecutando chequeo completo (descarga + análisis)")
            
            print("🚀 INICIANDO CHEQUEO COMPLETO DEL SISTEMA")
            print("="*60)
            
            # ETAPA 1: Descarga de adjuntos
            download_summary = run_download_stage(args.config, args.force_all, args.log_level)
            
            # ETAPA 2: Análisis de archivos
            analysis_summary = run_analysis_stage(args.config, args.log_level)
            
            # RESUMEN FINAL
            print("\n" + "="*60)
            print("📊 RESUMEN FINAL DEL CHEQUEO")
            print("="*60)
            print(f"📧 Emails procesados: {download_summary.get('emails_processed', 0)}")
            print(f"📎 Adjuntos descargados: {download_summary.get('files_downloaded', 0)}")
            print(f"📁 Archivos analizados: {analysis_summary.get('total_measurements', 0)}")
            print(f"👥 Pacientes procesados: {analysis_summary.get('patients_analyzed', 0)}")
            
            total_errors = len(download_summary.get('errors', [])) + len(analysis_summary.get('errors', []))
            if total_errors > 0:
                print(f"❌ Total de errores: {total_errors}")
            else:
                print("✅ Proceso completado sin errores")
            
            logger.info("Chequeo completo finalizado exitosamente")
            
        elif args.mode == 'download':
            # Solo descarga
            logger.info("Ejecutando solo descarga de adjuntos")
            run_download_stage(args.config, args.force_all, args.log_level)
            
        elif args.mode == 'analyze':
            # Solo análisis
            logger.info("Ejecutando solo análisis de archivos")
            run_analysis_stage(args.config, args.log_level)
            
        elif args.mode == 'dashboard':
            # Modo dashboard interactivo
            logger.info("Iniciando dashboard web")
            
            # Importar y ejecutar Streamlit
            import subprocess
            dashboard_cmd = [sys.executable, '-m', 'streamlit', 'run', 'dashboard.py']
            subprocess.run(dashboard_cmd)
            
        elif args.mode == 'report':
            # Modo solo generación de reportes
            logger.info("Generando reportes basados en datos existentes")
            
            try:
                from file_analyzer import FileAnalyzer
                analyzer = FileAnalyzer()
                
                # Cargar datos existentes y generar reporte
                analysis_summary = analyzer.analyze_all_downloaded_files()
                
                if analysis_summary.get('patients'):
                    analyzer.save_report(analysis_summary['completeness_report'])
                    analyzer.export_to_excel(analysis_summary['completeness_report'])
                    print("📋 Reportes generados en directorio 'reports/'")
                else:
                    print("No hay datos disponibles para generar reportes")
                    logger.warning("No se encontraron datos para generar reportes")
            except Exception as e:
                logger.error(f"Error generando reportes: {e}")
                print(f"Error generando reportes: {e}")
    
    except KeyboardInterrupt:
        logger.info("Proceso interrumpido por el usuario")
        print("\nProceso interrumpido por el usuario")
        
    except Exception as e:
        logger.error(f"Error inesperado: {e}", exc_info=True)
        print(f"Error inesperado: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
