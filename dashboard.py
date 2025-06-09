import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
from datetime import datetime, timedelta
from monitoring_system import MonitoringSystem
import logging

# Configurar página
st.set_page_config(
    page_title="Sistema de Monitoreo Médico",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configurar logging para Streamlit
logging.basicConfig(level=logging.INFO)

class MedicalDashboard:
    def __init__(self):
        """Inicializa el dashboard médico"""
        try:
            # MonitoringSystem solo acepta config_file como parámetro
            self.monitoring_system = MonitoringSystem("config.json")
            self.reports_path = self.monitoring_system.reports_path
        except Exception as e:
            st.error(f"Error inicializando sistema de monitoreo: {e}")
            self.monitoring_system = None
            self.reports_path = 'reports'
    
    def has_consecutive_complete_days(self, daily_data, requirements):
        """
        Verifica si el paciente tiene 7 días consecutivos completos
        
        Args:
            daily_data: Datos diarios del paciente
            requirements: Requerimientos de mediciones por franja
            
        Returns:
            tuple: (tiene_7_dias_consecutivos, max_dias_consecutivos)
        """
        if not daily_data:
            return False, 0
        
        # Obtener todas las fechas y ordenarlas
        dates = sorted(daily_data.keys())
        
        # Convertir a objetos datetime para facilitar el cálculo
        date_objects = []
        for date_str in dates:
            try:
                date_obj = datetime.fromisoformat(date_str).date()
                date_objects.append(date_obj)
            except:
                continue
        
        if not date_objects:
            return False, 0
        
        date_objects = sorted(set(date_objects))  # Eliminar duplicados y ordenar
        
        # Verificar qué días están completos (ambas franjas con mediciones suficientes)
        complete_days = []
        for date_obj in date_objects:
            date_str = date_obj.isoformat()
            if date_str in daily_data:
                day_data = daily_data[date_str]
                
                # Verificar si ambas franjas están completas
                matutina_complete = False
                vespertina_complete = False
                
                if 'matutina' in day_data:
                    matutina_count = day_data['matutina'].get('pressure_count', 0)
                    matutina_complete = matutina_count >= requirements.get('pressure_per_slot', 2)
                
                if 'vespertina' in day_data:
                    vespertina_count = day_data['vespertina'].get('pressure_count', 0)
                    vespertina_complete = vespertina_count >= requirements.get('pressure_per_slot', 2)
                
                # El día está completo si ambas franjas están completas
                if matutina_complete and vespertina_complete:
                    complete_days.append(date_obj)
        
        if not complete_days:
            return False, 0
        
        # Buscar la secuencia más larga de días consecutivos
        max_consecutive = 0
        current_consecutive = 1
        
        for i in range(1, len(complete_days)):
            # Verificar si el día actual es consecutivo al anterior
            if (complete_days[i] - complete_days[i-1]).days == 1:
                current_consecutive += 1
            else:
                max_consecutive = max(max_consecutive, current_consecutive)
                current_consecutive = 1
        
        max_consecutive = max(max_consecutive, current_consecutive)
        
        # Un paciente está completo si tiene 7 días consecutivos
        has_seven_consecutive = max_consecutive >= 7
        
        return has_seven_consecutive, max_consecutive
        
    def load_latest_report(self):
        """Carga el reporte más reciente"""
        if not self.monitoring_system:
            return None
            
        reports_path = self.reports_path
        
        if not os.path.exists(reports_path):
            return None
        
        # Buscar el archivo de reporte más reciente
        report_files = [f for f in os.listdir(reports_path) if f.startswith('monitoring_report_') and f.endswith('.json')]
        
        if not report_files:
            return None
        
        # Ordenar por fecha de modificación
        report_files.sort(key=lambda x: os.path.getmtime(os.path.join(reports_path, x)), reverse=True)
        latest_file = report_files[0]
        
        try:
            with open(os.path.join(reports_path, latest_file), 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            st.error(f"Error cargando reporte: {e}")
            return None
    
    def run_dashboard(self):
        """Ejecuta el dashboard principal"""
        st.title("🏥 Sistema de Monitoreo Médico")
        st.markdown("**Criterio de completitud: 7 días consecutivos con 2 mediciones de presión + 2 mediciones de ECG por franja horaria**")
        st.markdown("---")
        
        # Sidebar para controles
        with st.sidebar:
            st.header("⚙️ Controles")
            
            # Botón para ejecutar chequeo manual
            if st.button("🔄 Ejecutar Chequeo Manual", type="primary"):
                if not self.monitoring_system:
                    st.error("Sistema de monitoreo no disponible")
                    return
                    
                with st.spinner("Ejecutando chequeo..."):
                    try:
                        summary = self.monitoring_system.run_daily_check()
                        st.success("Chequeo completado exitosamente")
                        
                        # Mostrar resumen rápido
                        st.metric("Emails Procesados", summary.get('emails_processed', 0))
                        st.metric("Pacientes Procesados", summary.get('patients_processed', 0))
                        
                        if summary.get('errors'):
                            st.error(f"Errores encontrados: {len(summary['errors'])}")
                        
                        if summary.get('warnings'):
                            st.warning(f"Advertencias: {len(summary['warnings'])}")
                        
                        # Recargar la página para mostrar nuevos datos
                        st.rerun()
                        
                    except Exception as e:
                        st.error(f"Error ejecutando chequeo: {e}")
            
            st.markdown("---")
            
            # Información del criterio
            st.subheader("📋 Criterio de Completitud")
            st.info("""
            **Paciente Completo:**
            - 7 días consecutivos completos
            - Cada día: 2 franjas (matutina + vespertina)
            - Cada franja: 2 presiones + 2 ECGs
            
            **Franjas Horarias:**
            - Matutina: 04:00 - 12:59
            - Vespertina: 13:00 - 03:00
            """)
            
            # Configuración de filtros
            st.subheader("📊 Filtros")
            
            # Selector de período
            period_options = {
                "Últimos 7 días": 7,
                "Últimos 14 días": 14,
                "Último mes": 30
            }
            
            selected_period = st.selectbox(
                "Período de análisis",
                options=list(period_options.keys()),
                index=0
            )
            
            days_back = period_options[selected_period]
        
        # Cargar datos del reporte más reciente
        report_data = self.load_latest_report()
        
        if not report_data:
            st.warning("No hay datos de reporte disponibles. Ejecute un chequeo manual para generar datos.")
            return
        
        # Mostrar información del reporte
        report_date = datetime.fromisoformat(report_data['generation_date'].replace('Z', '+00:00'))
        st.info(f"📅 Último reporte generado: {report_date.strftime('%d/%m/%Y %H:%M:%S')}")
        
        # Métricas principales
        self.show_main_metrics(report_data)
        
        # Gráficos y análisis
        col1, col2 = st.columns(2)
        
        with col1:
            self.show_completion_chart(report_data)
            self.show_patient_status_table(report_data)
        
        with col2:
            self.show_timeline_chart(report_data)
            self.show_missing_measurements(report_data)
        
        # Tabla detallada de pacientes
        st.markdown("---")
        self.show_detailed_patient_table(report_data)
        
        # Alertas y recomendaciones
        st.markdown("---")
        self.show_alerts_and_recommendations(report_data)
    
    def show_main_metrics(self, report_data):
        """Muestra las métricas principales en la parte superior"""
        overall = report_data['overall_summary']
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "👥 Total Pacientes",
                overall['total_patients']
            )
        
        # Recalcular métricas con el criterio de 7 días consecutivos
        total_patients = overall['total_patients']
        total_slots = total_patients * 14  # 14 franjas por paciente
        
        # Contar franjas completas y pacientes completos (7 días consecutivos)
        complete_slots = 0
        patients_complete = 0
        
        for patient_name, patient_info in report_data['patients'].items():
            daily_data = patient_info.get('daily_data', {})
            requirements = patient_info.get('requirements', {'pressure_per_slot': 2})
            
            # Contar franjas completas para este paciente
            for date_str, day_data in daily_data.items():
                for time_slot, slot_data in day_data.items():
                    pressure_count = slot_data.get('pressure_count', 0)
                    if pressure_count >= requirements['pressure_per_slot']:
                        complete_slots += 1
            
            # Verificar si el paciente tiene 7 días consecutivos completos
            has_consecutive, max_consecutive = self.has_consecutive_complete_days(daily_data, requirements)
            if has_consecutive:
                patients_complete += 1
        
        patients_incomplete = total_patients - patients_complete
        
        with col2:
            completion_rate = (patients_complete / total_patients * 100) if total_patients > 0 else 0
            st.metric(
                "✅ Pacientes Completos",
                patients_complete,
                f"{completion_rate:.1f}%"
            )
        
        with col3:
            measurement_rate = (complete_slots / total_slots * 100) if total_slots > 0 else 0
            st.metric(
                "📊 Franjas Completas",
                f"{complete_slots}/{total_slots}",
                f"{measurement_rate:.1f}%"
            )
        
        with col4:
            st.metric(
                "⚠️ Pacientes Incompletos",
                patients_incomplete
            )
    
    def show_completion_chart(self, report_data):
        """Muestra gráfico de completitud por paciente"""
        st.subheader("📈 Completitud por Paciente")
        
        # Preparar datos
        patients_data = []
        for patient_name, patient_info in report_data['patients'].items():
            daily_data = patient_info.get('daily_data', {})
            requirements = patient_info.get('requirements', {'pressure_per_slot': 2})
            
            # Verificar días consecutivos
            has_consecutive, max_consecutive = self.has_consecutive_complete_days(daily_data, requirements)
            
            patients_data.append({
                'Paciente': patient_name,
                'Días Consecutivos': max_consecutive,
                'Estado': 'Completo (7+ días)' if has_consecutive else f'Incompleto ({max_consecutive}/7 días)'
            })
        
        if not patients_data:
            st.warning("No hay datos de pacientes disponibles")
            return
        
        df = pd.DataFrame(patients_data)
        
        # Crear gráfico de barras
        fig = px.bar(
            df,
            x='Paciente',
            y='Días Consecutivos',
            color='Estado',
            color_discrete_map={'Completo (7+ días)': '#28a745', 'Incompleto': '#dc3545'},
            title="Días Consecutivos Completos por Paciente (Requiere 7 días consecutivos)"
        )
        
        fig.update_layout(
            xaxis_tickangle=-45,
            height=400,
            showlegend=True
        )
        
        # Agregar línea de referencia en 7 días
        fig.add_hline(y=7, line_dash="dash", line_color="gray", annotation_text="Meta: 7 días consecutivos")
        
        st.plotly_chart(fig, use_container_width=True)
    
    def show_timeline_chart(self, report_data):
        """Muestra gráfico de timeline de mediciones"""
        st.subheader("📅 Timeline de Mediciones")
        
        # Preparar datos para el timeline
        timeline_data = []
        
        for patient_name, patient_info in report_data['patients'].items():
            daily_data = patient_info.get('daily_data', {})
            requirements = patient_info.get('requirements', {'pressure_per_slot': 2, 'ecg_per_slot': 2})
        
            for date_str, day_data in daily_data.items():
                for time_slot, slot_data in day_data.items():
                    # Contar mediciones de presión
                    pressure_count = slot_data.get('pressure_count', 0)
                    # Para ECG, asumir 2 si presión está completa (simplificación)
                    ecg_count = 2 if pressure_count >= requirements['pressure_per_slot'] else 0
                    
                    # Verificar si la franja está completa
                    is_complete = (pressure_count >= requirements['pressure_per_slot'] and 
                                 ecg_count >= requirements['ecg_per_slot'])
                    
                    if pressure_count > 0:
                        # Mapear las franjas a nombres más descriptivos
                        slot_display = {
                            'matutina': 'Matutina (04:00-12:59)',
                            'vespertina': 'Vespertina (13:00-03:00)'
                        }.get(time_slot, time_slot)
                    
                        timeline_data.append({
                            'Paciente': patient_name,
                            'Fecha': date_str,
                            'Franja': slot_display,
                            'Presión': pressure_count,
                            'ECG': ecg_count,
                            'Completo': is_complete,
                            'Estado': f"P:{pressure_count}/2, E:{ecg_count}/2"
                        })
    
        if not timeline_data:
            st.warning("No hay datos de timeline disponibles")
            return
        
        df_timeline = pd.DataFrame(timeline_data)
        df_timeline['Fecha'] = pd.to_datetime(df_timeline['Fecha'])
        
        # Crear gráfico de dispersión
        fig = px.scatter(
            df_timeline,
            x='Fecha',
            y='Paciente',
            color='Completo',
            symbol='Franja',
            hover_data=['Estado'],
            color_discrete_map={True: '#28a745', False: '#ffc107'},
            title="Timeline de Mediciones por Paciente (Verde: 2P+2E completo, Amarillo: incompleto)"
        )
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    def show_patient_status_table(self, report_data):
        """Muestra tabla de estado de pacientes"""
        st.subheader("👥 Estado de Pacientes")
        
        # Preparar datos para la tabla
        table_data = []
        for patient_name, patient_info in report_data['patients'].items():
            daily_data = patient_info.get('daily_data', {})
            requirements = patient_info.get('requirements', {'pressure_per_slot': 2})
            
            # Calcular franjas completas
            complete_slots = 0
            for date_str, day_data in daily_data.items():
                for time_slot, slot_data in day_data.items():
                    pressure_count = slot_data.get('pressure_count', 0)
                    if pressure_count >= requirements['pressure_per_slot']:
                        complete_slots += 1
            
            # Verificar días consecutivos
            has_consecutive, consecutive_days = self.has_consecutive_complete_days(daily_data, requirements)
            
            table_data.append({
                'Paciente': patient_name,
                'Franjas Completas': f"{complete_slots}/14",
                'Días Consecutivos': f"{consecutive_days}/7",
                'Estado': '✅ Completo' if has_consecutive else '⚠️ Incompleto'
            })
        
        if table_data:
            df_table = pd.DataFrame(table_data)
            st.dataframe(df_table, use_container_width=True, hide_index=True)
        else:
            st.warning("No hay datos de pacientes disponibles")
    
    def show_missing_measurements(self, report_data):
        """Muestra mediciones faltantes"""
        st.subheader("❌ Mediciones Faltantes")
        
        missing_data = []
        for patient_name, patient_info in report_data['patients'].items():
            daily_data = patient_info.get('daily_data', {})
            requirements = patient_info.get('requirements', {'pressure_per_slot': 2})
            
            for date_str, day_data in daily_data.items():
                for time_slot, slot_data in day_data.items():
                    pressure_count = slot_data.get('pressure_count', 0)
                    if pressure_count < requirements['pressure_per_slot']:
                        missing_count = requirements['pressure_per_slot'] - pressure_count
                        missing_data.append({
                            'Paciente': patient_name,
                            'Fecha': date_str,
                            'Franja': time_slot,
                            'Faltantes': f"{missing_count} mediciones de presión"
                        })
        
        if missing_data:
            df_missing = pd.DataFrame(missing_data)
            
            # Mostrar solo las más recientes (últimos 3 días)
            df_missing['Fecha'] = pd.to_datetime(df_missing['Fecha'])
            recent_date = df_missing['Fecha'].max() - timedelta(days=3)
            df_recent = df_missing[df_missing['Fecha'] >= recent_date]
            
            if not df_recent.empty:
                st.dataframe(df_recent.sort_values('Fecha', ascending=False), use_container_width=True, hide_index=True)
            else:
                st.success("No hay mediciones faltantes recientes")
        else:
            st.success("No hay mediciones faltantes")
    
    def show_detailed_patient_table(self, report_data):
        """Muestra tabla detallada expandible por paciente"""
        st.subheader("📋 Detalle por Paciente")
        
        for patient_name, patient_info in report_data['patients'].items():
            daily_data = patient_info.get('daily_data', {})
            requirements = patient_info.get('requirements', {'pressure_per_slot': 2})
            
            # Calcular franjas completas
            complete_slots = sum(1 for day_data in daily_data.values() 
                               for slot_data in day_data.values() 
                               if slot_data.get('pressure_count', 0) >= requirements['pressure_per_slot'])
            
            # Verificar días consecutivos
            has_consecutive, consecutive_days = self.has_consecutive_complete_days(daily_data, requirements)
            
            with st.expander(f"👤 {patient_name} - {consecutive_days}/7 días consecutivos"):
                
                # Métricas del paciente
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Franjas Completas", f"{complete_slots}/14")
                
                with col2:
                    st.metric("Días Consecutivos", f"{consecutive_days}/7")
                
                with col3:
                    status_color = "🟢" if has_consecutive else "🟡"
                    st.metric("Estado", f"{status_color} {'Completo' if has_consecutive else 'Incompleto'}")
                
                # Mostrar criterio de completitud
                st.info(f"**Criterio:** 7 días consecutivos con {requirements['pressure_per_slot']} presiones + 2 ECGs por franja")
                
                # Tabla de mediciones diarias
                if daily_data:
                    daily_table = []
                    
                    for date_str, day_data in daily_data.items():
                        for time_slot, slot_data in day_data.items():
                            # Mapear las franjas a nombres más descriptivos
                            slot_display = {
                                'matutina': 'Matutina (04:00-12:59)',
                                'vespertina': 'Vespertina (13:00-03:00)'
                            }.get(time_slot, time_slot)
                            
                            # Contar mediciones
                            pressure_count = slot_data.get('pressure_count', 0)
                            ecg_count = 2 if pressure_count >= requirements['pressure_per_slot'] else 0  # Simplificación
                            
                            # Verificar si la franja está completa
                            is_complete = pressure_count >= requirements['pressure_per_slot']
                            
                            # Crear indicadores de estado
                            pressure_status = f"✅ ({pressure_count}/{requirements['pressure_per_slot']})" if pressure_count >= requirements['pressure_per_slot'] else f"❌ ({pressure_count}/{requirements['pressure_per_slot']})"
                            ecg_status = f"✅ ({ecg_count}/2)" if ecg_count >= 2 else f"❌ ({ecg_count}/2)"
                            
                            daily_table.append({
                                'Fecha': date_str,
                                'Franja Horaria': slot_display,
                                'Presión': pressure_status,
                                'ECG': ecg_status,
                                'Completo': '✅' if is_complete else '❌'
                            })
                    
                    if daily_table:
                        df_daily = pd.DataFrame(daily_table)
                        df_daily = df_daily.sort_values(['Fecha', 'Franja Horaria'])
                        st.dataframe(df_daily, use_container_width=True, hide_index=True)
                
                # Mostrar detalles de mediciones si están disponibles
                if daily_data:
                    st.write("**Detalles de Mediciones de Presión:**")
                    for date_str, day_data in daily_data.items():
                        st.write(f"📅 **{date_str}**")
                        for time_slot, slot_data in day_data.items():
                            pressure_data = slot_data.get('pressure_data', [])
                            if pressure_data:
                                st.write(f"  🕐 **{time_slot.title()}:**")
                                for i, measurement in enumerate(pressure_data, 1):
                                    st.write(f"    {i}. {measurement['time']} - {measurement['systolic']}/{measurement['diastolic']} mmHg, Pulso: {measurement['pulse']} bpm")
    
    def show_alerts_and_recommendations(self, report_data):
        """Muestra alertas y recomendaciones"""
        st.subheader("🚨 Alertas y Recomendaciones")
        
        alerts = []
        recommendations = []
        
        # Analizar datos para generar alertas basadas en días consecutivos
        for patient_name, patient_info in report_data['patients'].items():
            daily_data = patient_info.get('daily_data', {})
            requirements = patient_info.get('requirements', {'pressure_per_slot': 2})
            
            has_consecutive, consecutive_days = self.has_consecutive_complete_days(daily_data, requirements)
            
            if consecutive_days == 0:
                alerts.append(f"🔴 **{patient_name}**: Sin días completos - Revisar todas las mediciones")
            elif consecutive_days < 3:
                alerts.append(f"🟡 **{patient_name}**: Solo {consecutive_days} días consecutivos - Necesita continuidad")
            elif consecutive_days < 7:
                alerts.append(f"🟠 **{patient_name}**: {consecutive_days}/7 días consecutivos - Cerca de completar")
        
        # Generar recomendaciones
        if alerts:
            recommendations.append("📞 Contactar pacientes que no tienen 7 días consecutivos completos")
            recommendations.append("📋 Recordar la importancia de la continuidad en las mediciones")
            recommendations.append("📅 Verificar que las mediciones se realicen todos los días sin saltos")
        
        # Mostrar alertas
        if alerts:
            st.write("**Alertas Activas:**")
            for alert in alerts:
                st.markdown(alert)
        else:
            st.success("✅ No hay alertas activas")
        
        # Mostrar recomendaciones
        if recommendations:
            st.write("**Recomendaciones:**")
            for rec in recommendations:
                st.markdown(rec)

# Función principal para ejecutar el dashboard
def main():
    dashboard = MedicalDashboard()
    dashboard.run_dashboard()

if __name__ == "__main__":
    main()
