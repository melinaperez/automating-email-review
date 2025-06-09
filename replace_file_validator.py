#!/usr/bin/env python3
"""
Script para reemplazar el FileValidator con la versión mejorada
"""

import os
import shutil
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def replace_file_validator():
    """Reemplaza el FileValidator con la versión mejorada"""
    
    original_file = "file_validator.py"
    improved_file = "improved_file_validator.py"
    backup_file = "file_validator_original.py.bak"
    
    # Verificar que existe el archivo mejorado
    if not os.path.exists(improved_file):
        logger.error(f"No se encontró el archivo mejorado: {improved_file}")
        return False
    
    # Crear copia de seguridad del original
    if os.path.exists(original_file):
        shutil.copy2(original_file, backup_file)
        logger.info(f"Copia de seguridad creada: {backup_file}")
    
    # Reemplazar el archivo original con el mejorado
    shutil.copy2(improved_file, original_file)
    logger.info(f"FileValidator reemplazado con la versión mejorada")
    
    return True

def test_new_validator():
    """Prueba el nuevo FileValidator"""
    try:
        from file_validator import FileValidator
        
        # Crear instancia
        validator = FileValidator()
        
        # Verificar que tiene el resolvedor AM/PM
        if hasattr(validator, 'ampm_resolver'):
            logger.info("✅ Nuevo resolvedor AM/PM detectado")
            return True
        else:
            logger.error("❌ No se detectó el nuevo resolvedor AM/PM")
            return False
            
    except Exception as e:
        logger.error(f"Error probando el nuevo validator: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Reemplazando FileValidator con versión mejorada...")
    
    if replace_file_validator():
        print("✅ FileValidator reemplazado exitosamente")
        
        print("\n🧪 Probando nuevo FileValidator...")
        if test_new_validator():
            print("✅ Nuevo FileValidator funcionando correctamente")
        else:
            print("❌ Error en el nuevo FileValidator")
    else:
        print("❌ Error reemplazando FileValidator")
