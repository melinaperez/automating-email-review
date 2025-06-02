import pandas as pd
import PyPDF2
import pdfplumber
import os
import re
from datetime import datetime, time
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class FileValidator:
    def __init__(self):
        """Inicializa el validador de archivos"""
        # Definir franjas horarias
        self.time_slots = {
            'matutina_1': (time(6, 0), time(9, 0)),
            'matutina_2': (time(9, 1), time(12, 0)),
            'vespertina_1': (time(13, 0), time(17, 0)),
            'vespertina_2': (time(17, 1), time(21, 0))
        }
        
        # Rangos normales para validación
        self.pressure_ranges = {
            'systolic': (70, 250),
            'diastolic': (40, 150),
            'pulse': (40, 150)
        }
    
    def validate_csv_file(self, file_path: str) -> Dict:
        """
        Valida un archivo CSV de presión arterial
        
        Returns:
            Diccionario con resultado de validación
        """
        validation_result = {
            'file_path': file_path,
            'is_valid': False,
            'errors': [],
            'warnings': [],
            'data': None,
            'measurement_time': None,
            'time_slot': None
        }
        
        try:
            # Verificar que el archivo existe y es accesible
            if not os.path.exists(file_path):
                validation_result['errors'].append("Archivo no encontrado")
                return validation_result
            
            if os.path.getsize(file_path) == 0:
                validation_result['errors'].append("Archivo vacío")
                return validation_result
            
            # Intentar leer el CSV
            try:
                # Probar diferentes encodings
                encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
                df = None
                
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, encoding=encoding)
                        logger.info(f"CSV leído exitosamente con encoding {encoding}")
                        break
                    except UnicodeDecodeError:
                        continue
                
                if df is None:
                    validation_result['errors'].append("No se pudo leer el archivo con ninguna codificación")
                    return validation_result
                
                logger.info(f"CSV cargado: {len(df)} filas, columnas: {list(df.columns)}")
                
            except Exception as e:
                validation_result['errors'].append(f"Error leyendo CSV: {str(e)}")
                return validation_result
            
            # Validar estructura del CSV
            required_columns = self.detect_csv_columns(df)
            if not required_columns:
                validation_result['errors'].append("No se encontraron columnas de presión arterial válidas")
                return validation_result
            
            # Extraer datos de presión
            pressure_data = self.extract_pressure_data(df, required_columns)
            if not pressure_data:
                validation_result['errors'].append("No se pudieron extraer datos de presión válidos")
                return validation_result
            
            # Validar rangos de presión
            range_validation = self.validate_pressure_ranges(pressure_data)
            validation_result['warnings'].extend(range_validation['warnings'])
            
            # Extraer tiempo de medición del contenido del CSV
            measurement_time = self.extract_measurement_time_from_csv(df, file_path)
            if measurement_time:
                validation_result['measurement_time'] = measurement_time.isoformat()
                validation_result['time_slot'] = self.classify_time_slot(measurement_time)
                logger.info(f"Tiempo de medición extraído: {measurement_time} -> {validation_result['time_slot']}")
            else:
                validation_result['warnings'].append("No se pudo determinar la hora de medición")
                logger.warning(f"No se pudo extraer tiempo de medición de {file_path}")
            
            validation_result['data'] = pressure_data
            validation_result['is_valid'] = len(validation_result['errors']) == 0
            
        except Exception as e:
            validation_result['errors'].append(f"Error inesperado validando CSV: {str(e)}")
        
        return validation_result
    
    def detect_csv_columns(self, df: pd.DataFrame) -> Optional[Dict[str, str]]:
        """Detecta las columnas relevantes en el CSV"""
        columns = df.columns.str.lower()
        
        # Patrones para detectar columnas
        patterns = {
            'systolic': ['sistolic', 'systolic', 'sys', 'presion_sistolic', 'presión_sistólica', 'sys(mmhg)'],
            'diastolic': ['diastolic', 'diastolic', 'dia', 'presion_diastolic', 'presión_diastólica', 'dia(mmhg)'],
            'pulse': ['pulse', 'pulso', 'heart_rate', 'frecuencia', 'pulse(bpm)'],
            'date': ['date', 'fecha', 'timestamp', 'time', 'fecha de la medición', 'fecha de la medicion'],
            'time': ['time', 'hora', 'hour']
        }
        
        found_columns = {}
        
        for data_type, pattern_list in patterns.items():
            for col in columns:
                for pattern in pattern_list:
                    if pattern in col:
                        found_columns[data_type] = df.columns[columns.tolist().index(col)]
                        break
                if data_type in found_columns:
                    break
        
        logger.info(f"Columnas detectadas: {found_columns}")
        
        # Verificar que al menos tengamos presión sistólica y diastólica
        if 'systolic' in found_columns and 'diastolic' in found_columns:
            return found_columns
        
        return None
    
    def extract_pressure_data(self, df: pd.DataFrame, columns: Dict[str, str]) -> Optional[Dict]:
        """Extrae los datos de presión del DataFrame"""
        try:
            data = {}
            
            # Extraer valores (tomar el primer registro válido)
            for data_type, col_name in columns.items():
                if col_name in df.columns:
                    values = df[col_name].dropna()
                    if len(values) > 0:
                        # Intentar convertir a numérico
                        if data_type in ['systolic', 'diastolic', 'pulse']:
                            try:
                                data[data_type] = float(values.iloc[0])
                            except (ValueError, TypeError):
                                # Intentar extraer números del texto
                                text_value = str(values.iloc[0])
                                numbers = re.findall(r'\d+', text_value)
                                if numbers:
                                    data[data_type] = float(numbers[0])
                        else:
                            data[data_type] = values.iloc[0]
            
            logger.info(f"Datos de presión extraídos: {data}")
            return data if data else None
            
        except Exception as e:
            logger.error(f"Error extrayendo datos de presión: {e}")
            return None
    
    def extract_measurement_time_from_csv(self, df: pd.DataFrame, file_path: str) -> Optional[datetime]:
        """Extrae la hora de medición del contenido del CSV"""
        
        # 1. Buscar en las columnas de fecha/hora del CSV
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['fecha', 'date', 'time', 'hora', 'timestamp', 'medición', 'medicion']):
                try:
                    time_values = df[col].dropna()
                    if len(time_values) > 0:
                        time_str = str(time_values.iloc[0])
                        logger.info(f"Intentando parsear fecha de columna '{col}': '{time_str}'")
                        
                        # Intentar parsear diferentes formatos
                        time_formats = [
                            '%Y/%m/%d %H:%M',      # 2025/05/27 21:07
                            '%Y-%m-%d %H:%M:%S',   # 2025-05-27 21:07:00
                            '%d/%m/%Y %H:%M',      # 27/05/2025 21:07
                            '%m/%d/%Y %H:%M',      # 05/27/2025 21:07
                            '%Y/%m/%d %H:%M:%S',   # 2025/05/27 21:07:00
                            '%d-%m-%Y %H:%M',      # 27-05-2025 21:07
                            '%Y%m%d %H%M%S',       # 20250527 210700
                            '%H:%M:%S',            # 21:07:00
                            '%H:%M'                # 21:07
                        ]
                        
                        for fmt in time_formats:
                            try:
                                parsed_time = datetime.strptime(time_str, fmt)
                                # Si solo tenemos hora, usar fecha actual
                                if fmt in ['%H:%M:%S', '%H:%M']:
                                    today = datetime.now().date()
                                    parsed_time = datetime.combine(today, parsed_time.time())
                                
                                logger.info(f"Fecha parseada exitosamente: {parsed_time}")
                                return parsed_time
                            except ValueError:
                                continue
                        
                        # Intentar parsear con pandas
                        try:
                            parsed_time = pd.to_datetime(time_str)
                            if pd.notna(parsed_time):
                                # Convertir a datetime nativo de Python
                                if hasattr(parsed_time, 'to_pydatetime'):
                                    parsed_time = parsed_time.to_pydatetime()
                                logger.info(f"Fecha parseada con pandas: {parsed_time}")
                                return parsed_time
                        except Exception:
                            pass
                        
                except Exception as e:
                    logger.warning(f"Error procesando columna {col}: {e}")
                    continue
        
        # 2. Si no se encuentra en el contenido, intentar extraer del nombre del archivo
        filename = os.path.basename(file_path)
        logger.info(f"Intentando extraer fecha del nombre del archivo: {filename}")
        
        # Patrones para nombres de archivo
        patterns = [
            # BloodPressure_202505-202505.csv -> Mayo 2025
            r'BloodPressure_(\d{6})-\d{6}',
            # pressure_2025-05-27_21-07-00.csv
            r'pressure_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})',
            # Otros patrones
            r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})',
            r'(\d{4}-\d{2}-\d{2})',
            r'(\d{1,2})[:-](\d{2})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 1:
                        # Formato YYYYMM
                        date_str = groups[0]
                        if len(date_str) == 6:
                            year = int(date_str[:4])
                            month = int(date_str[4:6])
                            day = 15  # Día medio del mes
                            hour = 12  # Mediodía como hora predeterminada
                            result = datetime(year, month, day, hour, 0)
                            logger.info(f"Fecha extraída del nombre (YYYYMM): {result}")
                            return result
                        elif '-' in date_str:  # YYYY-MM-DD
                            result = datetime.strptime(date_str, '%Y-%m-%d')
                            result = result.replace(hour=12)  # Mediodía
                            logger.info(f"Fecha extraída del nombre (YYYY-MM-DD): {result}")
                            return result
                    elif len(groups) == 2:
                        # Formato fecha + hora
                        date_str, time_str = groups
                        datetime_str = f"{date_str} {time_str.replace('-', ':')}"
                        result = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                        logger.info(f"Fecha y hora extraídas del nombre: {result}")
                        return result
                    elif len(groups) == 6:
                        # Formato YYYYMMDD_HHMMSS
                        year, month, day, hour, minute, second = map(int, groups)
                        result = datetime(year, month, day, hour, minute, second)
                        logger.info(f"Fecha completa extraída del nombre: {result}")
                        return result
                except ValueError as e:
                    logger.warning(f"Error parseando fecha del nombre con patrón {pattern}: {e}")
                    continue
        
        # 3. Como último recurso, usar la fecha actual
        logger.warning(f"No se pudo extraer fecha de {file_path}, usando fecha actual")
        return datetime.now()
    
    def validate_pressure_ranges(self, pressure_data: Dict) -> Dict:
        """Valida que los valores de presión estén en rangos normales"""
        result = {'warnings': []}
        
        for measurement, value in pressure_data.items():
            if measurement in self.pressure_ranges:
                min_val, max_val = self.pressure_ranges[measurement]
                if not (min_val <= value <= max_val):
                    result['warnings'].append(
                        f"{measurement.title()}: {value} fuera del rango normal ({min_val}-{max_val})"
                    )
        
        return result
    
    def classify_time_slot(self, measurement_time: datetime) -> str:
        """Clasifica la hora de medición en una franja horaria"""
        measurement_time_only = measurement_time.time()
        
        for slot_name, (start_time, end_time) in self.time_slots.items():
            if start_time <= measurement_time_only <= end_time:
                return slot_name
        
        return 'fuera_de_horario'
    
    def validate_pdf_file(self, file_path: str) -> Dict:
        """
        Valida un archivo PDF de ECG
        
        Returns:
            Diccionario con resultado de validación
        """
        validation_result = {
            'file_path': file_path,
            'is_valid': False,
            'errors': [],
            'warnings': [],
            'patient_name': None,
            'measurement_time': None,
            'time_slot': None,
            'content_summary': None
        }
        
        try:
            # Verificar que el archivo existe
            if not os.path.exists(file_path):
                validation_result['errors'].append("Archivo no encontrado")
                return validation_result
            
            if os.path.getsize(file_path) == 0:
                validation_result['errors'].append("Archivo vacío")
                return validation_result
            
            # Intentar leer el PDF
            try:
                with pdfplumber.open(file_path) as pdf:
                    if len(pdf.pages) == 0:
                        validation_result['errors'].append("PDF sin páginas")
                        return validation_result
                    
                    # Extraer texto de todas las páginas
                    full_text = ""
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            full_text += page_text + "\n"
                    
                    if not full_text.strip():
                        validation_result['errors'].append("PDF sin contenido de texto legible")
                        return validation_result
                    
                    logger.info(f"Texto extraído del PDF: {full_text[:200]}...")
                    
                    # Analizar contenido
                    content_analysis = self.analyze_ecg_content(full_text, file_path)
                    validation_result.update(content_analysis)
                    
            except Exception as e:
                validation_result['errors'].append(f"Error leyendo PDF: {str(e)}")
                return validation_result
            
            validation_result['is_valid'] = len(validation_result['errors']) == 0
            
        except Exception as e:
            validation_result['errors'].append(f"Error inesperado validando PDF: {str(e)}")
        
        return validation_result
    
    def analyze_ecg_content(self, text: str, file_path: str) -> Dict:
        """Analiza el contenido del PDF de ECG"""
        analysis = {
            'patient_name': None,
            'measurement_time': None,
            'time_slot': None,
            'content_summary': {},
            'warnings': []
        }
        
        # Buscar nombre del paciente
        name_patterns = [
            r'[Pp]aciente\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\-\s]+)',
            r'[Nn]ombre:?\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\-\s]+)',
            r'[Pp]atient:?\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\-\s]+)',
            r'Registrado\s*([A-Za-zÁÉÍÓÚáéíóúñÑ\-\s]+)'
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, text)
            if match:
                name = match.group(1).strip()
                if len(name) > 2:
                    analysis['patient_name'] = name.title()
                    break
        
        # Buscar fecha y hora en el texto del PDF
        datetime_patterns = [
            # jueves, 22 de may de 2025, 8:15:26
            r'(\w+),\s*(\d{1,2})\s*de\s*(\w+)\s*de\s*(\d{4}),\s*(\d{1,2}):(\d{2}):(\d{2})',
            # 22 de mayo de 2025, 8:15:26
            r'(\d{1,2})\s*de\s*(\w+)\s*de\s*(\d{4}),\s*(\d{1,2}):(\d{2}):(\d{2})',
            # 2025/05/22 08:15:26
            r'(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})\s+(\d{1,2}):(\d{2}):(\d{2})',
            # 22/05/2025 08:15
            r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\s+(\d{1,2}):(\d{2})',
            # Registrado + fecha
            r'Registrado\s*.*?(\d{1,2})\s*de\s*(\w+)\s*de\s*(\d{4}),\s*(\d{1,2}):(\d{2}):(\d{2})'
        ]
        
        months_es = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'may': 5,
            'junio': 6, 'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10,
            'noviembre': 11, 'diciembre': 12, 'ene': 1, 'feb': 2, 'mar': 3,
            'abr': 4, 'jun': 6, 'jul': 7, 'ago': 8, 'sep': 9, 'oct': 10,
            'nov': 11, 'dic': 12
        }
        
        for pattern in datetime_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    groups = match.groups()
                    logger.info(f"Grupos encontrados en PDF: {groups}")
                    
                    if len(groups) >= 6:
                        # Formato con día de semana y mes en español
                        if len(groups) == 7:  # Con día de semana
                            _, day, month_name, year, hour, minute, second = groups
                        else:  # Sin día de semana
                            day, month_name, year, hour, minute, second = groups[:6]
                        
                        month = months_es.get(month_name.lower())
                        if month:
                            measurement_time = datetime(
                                int(year), month, int(day),
                                int(hour), int(minute), int(second)
                            )
                            analysis['measurement_time'] = measurement_time.isoformat()
                            analysis['time_slot'] = self.classify_time_slot(measurement_time)
                            logger.info(f"Fecha extraída del PDF: {measurement_time} -> {analysis['time_slot']}")
                            break
                    elif len(groups) >= 5:
                        # Formato numérico
                        if '/' in groups[0] or '-' in groups[0]:
                            # Formato YYYY/MM/DD HH:MM:SS
                            year, month, day, hour, minute = groups[:5]
                            second = groups[5] if len(groups) > 5 else 0
                        else:
                            # Formato DD/MM/YYYY HH:MM
                            day, month, year, hour, minute = groups[:5]
                            second = 0
                        
                        measurement_time = datetime(
                            int(year), int(month), int(day),
                            int(hour), int(minute), int(second)
                        )
                        analysis['measurement_time'] = measurement_time.isoformat()
                        analysis['time_slot'] = self.classify_time_slot(measurement_time)
                        logger.info(f"Fecha extraída del PDF: {measurement_time} -> {analysis['time_slot']}")
                        break
                        
                except Exception as e:
                    logger.warning(f"Error parseando fecha del PDF: {e}")
                    continue
        
        # Si no se encuentra fecha en el texto, intentar extraer del nombre del archivo
        if not analysis['measurement_time']:
            filename = os.path.basename(file_path)
            logger.info(f"Intentando extraer fecha del nombre del archivo PDF: {filename}")
            
            # Patrones para nombres de archivo PDF
            patterns = [
                r'ecg_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})',
                r'(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})',
                r'(\d{4}-\d{2}-\d{2})',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, filename)
                if match:
                    try:
                        groups = match.groups()
                        if len(groups) == 2:
                            # Formato fecha + hora
                            date_str, time_str = groups
                            datetime_str = f"{date_str} {time_str.replace('-', ':')}"
                            measurement_time = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                        elif len(groups) == 6:
                            # Formato YYYYMMDD_HHMMSS
                            year, month, day, hour, minute, second = map(int, groups)
                            measurement_time = datetime(year, month, day, hour, minute, second)
                        elif len(groups) == 1:
                            # Solo fecha
                            measurement_time = datetime.strptime(groups[0], '%Y-%m-%d')
                            measurement_time = measurement_time.replace(hour=12)  # Mediodía
                        
                        analysis['measurement_time'] = measurement_time.isoformat()
                        analysis['time_slot'] = self.classify_time_slot(measurement_time)
                        logger.info(f"Fecha extraída del nombre del archivo PDF: {measurement_time} -> {analysis['time_slot']}")
                        break
                    except ValueError as e:
                        logger.warning(f"Error parseando fecha del nombre del archivo PDF: {e}")
                        continue
        
        # Analizar contenido del ECG
        ecg_keywords = ['ecg', 'electrocardiogram', 'ritmo', 'frecuencia', 'bpm', 'latido', 'cardíaca']
        found_keywords = [kw for kw in ecg_keywords if kw.lower() in text.lower()]
        
        analysis['content_summary'] = {
            'text_length': len(text),
            'ecg_keywords_found': found_keywords,
            'has_ecg_content': len(found_keywords) > 0
        }
        
        if not analysis['content_summary']['has_ecg_content']:
            analysis['warnings'].append("No se detectó contenido típico de ECG en el PDF")
        
        return analysis

# Ejemplo de uso
if __name__ == "__main__":
    validator = FileValidator()
    
    # Ejemplo de validación de CSV
    csv_result = validator.validate_csv_file("data/Juan_Perez/2025-05-25/pressure_2025-05-25_08-00.csv")
    print("Validación CSV:", csv_result)
    
    # Ejemplo de validación de PDF
    pdf_result = validator.validate_pdf_file("data/Juan_Perez/2025-05-25/ecg_2025-05-25_08-05.pdf")
    print("Validación PDF:", pdf_result)
