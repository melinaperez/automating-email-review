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
    """Configura el sistema de logging"""
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Crear directorio de logs si no existe
    os.makedirs('logs', exist_ok=True)
    
    # Configurar logging a archivo y consola
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.FileHandler(f'logs/monitoring_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(description='Sistema de Monitoreo Médico')
    parser.add_argument('--mode', choices=['check', 'dashboard', 'report'], 
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
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    logger.info(f"Iniciando sistema de monitoreo médico en modo: {args.mode}")
    
    try:
        if args.mode == 'check':
            # Modo de chequeo automático
            logger.info("Ejecutando chequeo automático de emails y archivos")
            
            system = MonitoringSystem(args.config)
            summary = system.run_daily_check(force_all_emails=args.force_all)
            
            # DEBUG: Mostrar datos de pacientes
            system.debug_patient_data()

            # Mostrar resumen
            print("\n" + "="*50)
            print("RESUMEN DEL CHEQUEO DIARIO")
            print("="*50)
            print(f"Emails procesados: {summary['emails_processed']}")
            print(f"Archivos validados: {summary['files_validated']}")
            print(f"Errores encontrados: {len(summary['errors'])}")
            print(f"Advertencias: {len(summary['warnings'])}")
            
            if summary['errors']:
                print("\nERRORES:")
                for error in summary['errors']:
                    print(f"  - {error}")
            
            if summary['warnings']:
                print("\nADVERTENCIAS:")
                for warning in summary['warnings']:
                    print(f"  - {warning}")
            
            # Generar reportes
            if 'completeness_report' in summary:
                report_path = system.save_report(summary['completeness_report'])
                excel_path = system.export_to_excel(summary['completeness_report'])
                
                print(f"\nReportes generados:")
                print(f"  - JSON: {report_path}")
                print(f"  - Excel: {excel_path}")
            
            logger.info("Chequeo completado exitosamente")
            
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
            
            system = MonitoringSystem(args.config)
            
            # Cargar datos existentes y generar reporte
            if system.patient_data:
                report = system.generate_completeness_report()
                report_path = system.save_report(report)
                excel_path = system.export_to_excel(report)
                
                print(f"Reportes generados:")
                print(f"  - JSON: {report_path}")
                print(f"  - Excel: {excel_path}")
            else:
                print("No hay datos disponibles para generar reportes")
                logger.warning("No se encontraron datos para generar reportes")
    
    except KeyboardInterrupt:
        logger.info("Proceso interrumpido por el usuario")
        print("\nProceso interrumpido por el usuario")
        
    except Exception as e:
        logger.error(f"Error inesperado: {e}", exc_info=True)
        print(f"Error inesperado: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
