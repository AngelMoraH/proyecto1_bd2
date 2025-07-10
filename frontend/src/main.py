import flet as ft
import time
import json
import requests


class SQLQueryApp:
    def __init__(self):
        pass
        # self.data = load_sample_data()

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

        self.tables_list = ft.ListView(
            expand=True,
            spacing=2,
            padding=10,
        )
        
        def load_tables():
            try:
                response = requests.get("http://localhost:8000/tables")
                if response.status_code == 200:
                    
                    tables = response.json()["tables"]
                    self.tables_list.controls.clear()
                    
                    for table in tables:
                        self.tables_list.controls.append(
                            ft.Container(
                                content=ft.Text(table, size=14),
                                padding=ft.padding.symmetric(vertical=8, horizontal=10),
                                border_radius=5,
                                on_click=lambda e, t=table: insert_table_name(t),
                                ink=True,
                            )
                        )
                else:
                    # Tablas de ejemplo en caso de error
                    example_tables = ["productos", "clientes", "ventas", "empleados", "categorias"]
                    self.tables_list.controls.clear()
                    
                    for table in example_tables:
                        self.tables_list.controls.append(
                            ft.Container(
                                content=ft.Text(table, size=14),
                                padding=ft.padding.symmetric(vertical=8, horizontal=10),
                                border_radius=5,
                                on_click=lambda e, t=table: insert_table_name(t),
                                ink=True,
                            )
                        )
            except Exception as e:
                # Tablas de ejemplo en caso de error
                example_tables = ["productos", "clientes", "ventas", "empleados", "categorias"]
                self.tables_list.controls.clear()
                
                for table in example_tables:
                    self.tables_list.controls.append(
                        ft.Container(
                            content=ft.Text(table, size=14),
                            padding=ft.padding.symmetric(vertical=8, horizontal=10),
                            border_radius=5,
                            on_click=lambda e, t=table: insert_table_name(t),
                            ink=True,
                        )
                    )
            page.update()

        # Función para insertar el nombre de la tabla en el campo de consulta
        def insert_table_name(table_name):
            current_position = self.query_field.selection.start
            current_text = self.query_field.value
            
            if current_position is not None:
                # Insertar el nombre de la tabla en la posición actual del cursor
                new_text = current_text[:current_position] + table_name + current_text[current_position:]
                self.query_field.value = new_text
                # Mover el cursor después del nombre de la tabla insertado
                self.query_field.selection = ft.TextSelection(
                    start=current_position + len(table_name),
                    end=current_position + len(table_name)
                )
            else:
                # Si no hay posición del cursor, añadir al final
                if self.query_field.value:
                    self.query_field.value += " " + table_name
                else:
                    self.query_field.value = table_name
            
            page.update()

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
        self.error_message = ft.Text(
            "", size=14, color=ft.Colors.RED_600, visible=False
        )

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
                        controls=[self.results_table],
                        scroll="auto",  # ← scroll horizontal
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
                response = requests.post(
                    "http://localhost:8000/sql_parser", json={"query": query}
                )
                if response.status_code != 200:
                    raise Exception("Error al consultar la API")
                print(response.json())
                results = response.json()["result"]
                execution_time_ms = response.json()["execution_time_seconds"]

                

                # Calcular tiempo de ejecución
                end_time = time.time()
                self.execution_time.value = (
                    f"Tiempo de ejecución: {execution_time_ms:.2f} ms"
                )
                print("results:", results, type(results))
                if isinstance(results, dict):
                    message = results.get("message", "Operación realizada.")
                    print("message:", message)
                    self.error_message.value = message
                    self.error_message.color = ft.Colors.GREEN_700
                    self.error_message.visible = True
                    self.results_table.columns.clear()
                    self.results_table.rows.clear()
                    self.results_table.columns.append(
                        ft.DataColumn(ft.Text(""))
                    )
                    self.results_container.visible = False
                    page.update()
                    return
                
                if not results:
                    raise Exception("La consulta no devolvió resultados")
                
                columns = list(results[0].keys())

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
                self.error_message.visible = False

            except Exception as e:
                # Calcular tiempo de ejecución
                end_time = time.time()
                execution_time_ms = (end_time - start_time) * 1000
                self.execution_time.value = (
                    f"Tiempo de ejecución: {execution_time_ms:.2f} ms"
                )

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

        # Botón para refrescar la lista de tablas
        refresh_button = ft.IconButton(
            icon=ft.Icons.REFRESH,
            tooltip="Refrescar lista de tablas",
            on_click=lambda _: load_tables(),
        )

        # Contenedor para la lista de tablas
        tables_container = ft.Container(
            content=ft.Column(
                [
                    ft.Row(
                        [
                            ft.Text("Tablas Disponibles", weight=ft.FontWeight.BOLD),
                            refresh_button,
                        ],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                    ft.Divider(),
                    self.tables_list,
                ],
                expand=True,
            ),
            width=200,
            border=ft.border.all(1, ft.Colors.GREY_400),
            border_radius=5,
            padding=10,
            margin=ft.margin.only(right=20),
        )

        # Contenedor para el área de consulta y resultados
        query_results_container = ft.Column(
            [
                ft.Text("Ejecutor de Consultas SQL", size=24, weight=ft.FontWeight.BOLD),
                ft.Container(height=20),
                ft.Container(
                    content=self.query_field,
                    border=ft.border.all(1, ft.Colors.GREY_400),
                    border_radius=5,
                    padding=10,
                ),
                ft.Container(
                    content=ft.Row(
                        [
                            execute_button,
                            self.execution_time,
                        ],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                    margin=ft.margin.only(top=10),
                ),
                self.error_message,
                self.results_container,
            ],
            expand=True,
        )

        # Estructura principal con dos columnas
        page.add(
            ft.Row(
                [
                    tables_container,
                    query_results_container,
                ],
                expand=True,
            )
        )
        
        # Cargar las tablas al iniciar
        load_tables()


# Iniciar la aplicación
app = SQLQueryApp()
ft.app(target=app.main)
