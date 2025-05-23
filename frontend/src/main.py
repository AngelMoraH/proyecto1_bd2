import flet as ft
import time
import json
import requests
# Simulación de base de datos (en un proyecto real, esto se conectaría a tu BD)
def load_sample_data():
    base_url = "http://localhost:8000/sql_parser"
    response = requests.post(base_url, json={"query": "SELECT * FROM productos"})
    if response.status_code == 200:
        data = response.json()
        return {
            "productos": data["result"],
            "tiendas": [
                {"id": 1, "nombre": "TechStore", "direccion": "Calle Principal 123", "lat": 14.6349, "lng": -90.5069},
                {"id": 2, "nombre": "MegaElectronics", "direccion": "Av. Central 456", "lat": 14.6276, "lng": -90.5165},
                {"id": 3, "nombre": "SmartWorld", "direccion": "Plaza Comercial 789", "lat": 14.6410, "lng": -90.5133},
                {"id": 4, "nombre": "FashionStore", "direccion": "Centro Comercial Este", "lat": 14.6289, "lng": -90.4964},
            ]
        }
    else:
        print("Error al cargar los datos de la API")
        return {
            "productos": [
                {"id": 1, "nombre": "Smartphone Galaxy S23", "categoria": "tecnologia", "precio": 899.99},
                {"id": 2, "nombre": "Laptop UltraBook Pro", "categoria": "tecnologia", "precio": 1299.99},
                {"id": 3, "nombre": "Camiseta Premium", "categoria": "ropa", "precio": 29.99},
                {"id": 4, "nombre": "Zapatillas Running Pro", "categoria": "ropa", "precio": 119.99},
                {"id": 5, "nombre": "Refrigerador Smart", "categoria": "hogar", "precio": 1499.99},
                {"id": 6, "nombre": "Auriculares Noise Cancelling", "categoria": "tecnologia", "precio": 249.99},
            ],
            "tiendas": [
                {"id": 1, "nombre": "TechStore", "direccion": "Calle Principal 123", "lat": 14.6349, "lng": -90.5069},
                {"id": 2, "nombre": "MegaElectronics", "direccion": "Av. Central 456", "lat": 14.6276, "lng": -90.5165},
                {"id": 3, "nombre": "SmartWorld", "direccion": "Plaza Comercial 789", "lat": 14.6410, "lng": -90.5133},
                {"id": 4, "nombre": "FashionStore", "direccion": "Centro Comercial Este", "lat": 14.6289, "lng": -90.4964},
            ]
        }

class SQLQueryApp:
    def __init__(self):
        pass
        #self.data = load_sample_data()
    def main(self, page: ft.Page):
        # Configuración de la página
        page.title = "Ejecutor de Consultas SQL"
        page.theme_mode = ft.ThemeMode.LIGHT
        page.padding = 20
        page.window_width = 1000
        page.window_height = 800
        page.theme = ft.Theme(
            color_scheme_seed=ft.Colors.BLUE,
        )
        
        # Área de texto para la consulta SQL
        self.query_field = ft.TextField(
            hint_text="Ingresa tu consulta SQL aquí... (Ejemplo: SELECT * FROM productos)",
            multiline=True,
            min_lines=5,
            max_lines=10,
            expand=True,
            border_radius=5,
        )
        
        # Indicador de tiempo de ejecución
        self.execution_time = ft.Text("", size=14, color=ft.Colors.GREY_700)
        
        # Mensaje de error
        self.error_message = ft.Text("", size=14, color=ft.Colors.RED_600, visible=False)
        
        # Tabla de resultados
        self.results_table = ft.DataTable(
            columns=[
        ft.DataColumn(ft.Text("")),
    ],
            border=ft.border.all(1, ft.Colors.GREY_400),
            border_radius=5,
            vertical_lines=ft.border.BorderSide(1, ft.Colors.GREY_300),
            horizontal_lines=ft.border.BorderSide(1, ft.Colors.GREY_300),
            column_spacing=20,
        )
        
        # Contenedor para la tabla de resultados
        self.results_container = ft.Container(
            content=ft.Column(
                controls=[
                    ft.Row(
                        controls=[
                            self.results_table
                        ],
                        scroll="auto"  # ← scroll horizontal
                    )
                ],
                scroll="auto",  # ← scroll vertical
                expand=True,
            ),
            expand=True,
            visible=False,
            margin=ft.margin.only(top=20),
            border=ft.border.all(1, ft.Colors.BLACK),
        )
        
        # Función para ejecutar la consulta
        def execute_query(e):
            query = self.query_field.value.strip()
            if not query:
                return
            
            # Limpiar resultados anteriores
            self.results_table.rows.clear()
            self.results_table.columns.clear()
            self.error_message.visible = False
            
            # Medir tiempo de ejecución
            start_time = time.time()
            
            try:
                # Simulación de resultados basados en la consulta
                results = []
                columns = []
                
                query_lower = query.lower()
                response = requests.post("http://localhost:8000/sql_parser", json={"query": query.upper()})
                if response.status_code != 200:
                    raise Exception("Error al consultar la API")

                results = response.json()["result"]
                if not results:
                    raise Exception("La consulta no devolvió resultados")

                columns = list(results[0].keys())

                # Calcular tiempo de ejecución
                end_time = time.time()
                execution_time_ms = (end_time - start_time) * 1000
                self.execution_time.value = f"Tiempo de ejecución: {execution_time_ms:.2f} ms"
                
                # Configurar columnas de la tabla
                self.results_table.columns = [
                    ft.DataColumn(ft.Text(col, weight=ft.FontWeight.BOLD)) 
                    for col in columns
                ]
                
                # Configurar filas de la tabla
                for row in results:
                    self.results_table.rows.append(
                        ft.DataRow(
                            cells=[
                                ft.DataCell(ft.Text(str(row.get(col, "")))) 
                                for col in columns
                            ]
                        )
                    )
                
                # Mostrar la tabla de resultados
                self.results_container.visible = True
                
            except Exception as e:
                # Calcular tiempo de ejecución
                end_time = time.time()
                execution_time_ms = (end_time - start_time) * 1000
                self.execution_time.value = f"Tiempo de ejecución: {execution_time_ms:.2f} ms"
                
                # Mostrar mensaje de error
                self.error_message.value = str(e)
                self.error_message.visible = True
                self.results_container.visible = False
            
            page.update()
        
        # Botón para ejecutar la consulta
        execute_button = ft.ElevatedButton(
            "Ejecutar Consulta",
            icon=ft.Icons.PLAY_ARROW,
            on_click=execute_query,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=5),
            ),
            width=200,
        )
        
        # Estructura principal
        page.add(
            ft.Text("Ejecutor de Consultas SQL", size=24, weight=ft.FontWeight.BOLD),
            ft.Container(height=20),
            ft.Container(
                content=self.query_field,
                border=ft.border.all(1, ft.Colors.GREY_400),
                border_radius=5,
                padding=10,
            ),
            ft.Container(
                content=ft.Row([
                    execute_button,
                    self.execution_time,
                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                margin=ft.margin.only(top=10),
            ),
            self.error_message,
            self.results_container,
        )

# Iniciar la aplicación
app = SQLQueryApp()
ft.app(target=app.main)
