import flet as ft
import time
import requests  # Para llamar al endpoint de FastAPI
import random

# --- Funciones de simulación de backend para búsqueda por imagen ---
def simular_busqueda_imagen(imagen_bytes: bytes) -> list[dict]:
    """
    Simula búsqueda de ropa por imagen.
    """
    time.sleep(random.uniform(0.5, 1.5))
    results = []
    for i in range(9):
        results.append({
            "url": f"https://via.placeholder.com/150?text=ImageSearch{i+1}",
            "similarity": round(random.uniform(70, 99), 2)
        })
    return results


def main(page: ft.Page):
    page.theme_mode = ft.ThemeMode.LIGHT
    page.bgcolor = ft.Colors.BLUE_GREY_50
    page.title = "Sistema de Búsqueda de Ropa"
    page.vertical_alignment = ft.CrossAxisAlignment.START
    page.horizontal_alignment = ft.CrossAxisAlignment.CENTER
    page.window_width = 1000
    page.window_height = 700
    page.window_min_width = 800
    page.window_min_height = 600

    # Etiquetas de tiempo
    text_search_time_label = ft.Text("Tiempo de ejecución: 0.0 ms", size=12, color=ft.Colors.BLACK87)
    image_search_time_label = ft.Text("Tiempo de ejecución: 0.0 ms", size=12, color=ft.Colors.BLACK87)

    # Grid de resultados
    results_grid = ft.GridView(
        runs_count=3,
        max_extent=200,
        child_aspect_ratio=1.0,
        spacing=10,
        run_spacing=10,
        padding=10,
        expand=True,
    )
    results_grid_container = ft.Column(
        controls=[results_grid],
        height=450,
        scroll=ft.ScrollMode.AUTO,
    )

    def update_results_grid(results: list[dict], is_image_search: bool):
        """Actualiza el GridView con los nuevos resultados."""
        results_grid.controls.clear()
        for item in results:
            image_card_content = [
                ft.Image(
                    src=item["url"],
                    fit=ft.ImageFit.COVER,
                    expand=True,
                )
            ]
            # Mostrar porcentaje o score
            label = f"{item['similarity']:.2f}" + ("%" if is_image_search else "")
            image_card_content.append(
                ft.Text(label, size=14, weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER)
            )

            results_grid.controls.append(
                ft.Card(
                    content=ft.Container(
                        content=ft.Column(
                            image_card_content,
                            alignment=ft.MainAxisAlignment.CENTER,
                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                            spacing=5,
                        ),
                        padding=5,
                    ),
                    elevation=2,
                )
            )
        page.update()

    # --- Sección de búsqueda por texto ---
    text_input = ft.TextField(
        hint_text="¿Qué te gustaría comprar?",
        expand=True,
        border_radius=8,
        border_color=ft.Colors.GREY_300,
        focused_border_color=ft.Colors.BLUE_ACCENT_400,
    )
    k_input = ft.TextField(
        hint_text="Por ejemplo: 5",
        width=150,
        keyboard_type=ft.KeyboardType.NUMBER,
    )
    search_method_dropdown = ft.Dropdown(
        width=200,
        value="knn",
        options=[
            ft.dropdown.Option("knn", "KNN Secuencial"),
            ft.dropdown.Option("inverted", "Índice Invertido"),
        ],
    )
    def on_text_search_click(e):
        query = text_input.value.strip()
        if not query:
            text_search_time_label.value = "Por favor, ingresa texto para buscar."
            page.update()
            return

        try:
            top_k = int(k_input.value)
        except (TypeError, ValueError):
            top_k = 10

        url = "http://127.0.0.1:8000/search_text"
        payload = {"query": query, "top_k": top_k}

        start_time = time.time()
        try:
            resp = requests.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            time_response = data.get("execution_time_ms", 0)

            hits = data.get("result", {}).get("response", [])
            results = []
            for item in hits:
                results.append({
                    "url": item["link"],
                    "similarity": item.get("score", 0)
                })
            text_search_time_label.value = f"Tiempo de ejecución: {time_response:.2f} ms"
            update_results_grid(results, False)

        except Exception as ex:
            text_search_time_label.value = f"Error en búsqueda: {ex}"
        page.update()


    text_search_section = ft.Container(
        content=ft.Column(
            [
                ft.Text("Texto", size=20, weight=ft.FontWeight.BOLD),
                ft.Row([text_input]),
                ft.ElevatedButton(
                    text="Buscar",
                    icon=ft.Icons.SEARCH,
                    on_click=on_text_search_click,
                    style=ft.ButtonStyle(
                        shape=ft.RoundedRectangleBorder(radius=8),
                        padding=ft.padding.symmetric(horizontal=20, vertical=10),
                    ),
                ),
                text_search_time_label,
            ],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=15,
        ),
        padding=20,
        margin=ft.margin.only(bottom=20),
        border_radius=10,
        bgcolor=ft.Colors.WHITE70,
        shadow=ft.BoxShadow(spread_radius=1, blur_radius=5, color=ft.Colors.BLACK12, offset=ft.Offset(0, 2)),
    )

    # --- Sección de búsqueda por imagen ---
    file_picker = ft.FilePicker(on_result=lambda e: on_image_upload_result(e))
    page.overlay.append(file_picker)

    def on_image_upload_click(e):
        file_picker.pick_files(allow_multiple=False, allowed_extensions=["jpg", "jpeg", "png", "gif"])

    def on_image_upload_result(e: ft.FilePickerResultEvent):
        if e.files:
            selected_file = e.files[0]
           

            try:
                top_k = int(k_input.value) if k_input.value else 10
            except (TypeError, ValueError):
                top_k = 10

            method = search_method_dropdown.value
            endpoint = f"http://127.0.0.1:8000/search_image_{method}"
            
            try:
                with open(selected_file.path, "rb") as f:
                    files = {"file": (selected_file.name, f, "image/jpeg")}
                    params = {"top_k": top_k}
                    start_time = time.time()
                    response = requests.post(endpoint, files=files, params=params)
                    response.raise_for_status()
                    
                    result_data = response.json()
                    execution_time_ms = result_data.get("execution_time_ms", 0)
                    
                    hits = result_data.get("result", {}).get("response", [])
                    results = []
                    for item in hits:
                        results.append({
                            "url": item["link"],
                            "similarity": item.get("similarity", 0) * 100
                        })
                    
                    image_search_time_label.value = f"Tiempo de ejecución: {execution_time_ms:.2f} ms"
                    update_results_grid(results, True)
                    
            except Exception as ex:
                image_search_time_label.value = f"Error en búsqueda por imagen: {ex}"
        else:
            image_search_time_label.value = "No se seleccionó ninguna imagen."
        page.update()

    image_search_section = ft.Container(
        content=ft.Column(
            [
                ft.Text("Imagen", size=20, weight=ft.FontWeight.BOLD),
                ft.Text("Método de búsqueda:", size=14),
                search_method_dropdown,
                ft.ElevatedButton(
                    content=ft.Row(
                        [
                            ft.Icon(ft.Icons.UPLOAD),
                            ft.Text("Subir imagen", size=16),
                        ],
                        spacing=8,
                        alignment=ft.MainAxisAlignment.CENTER,
                    ),
                    on_click=on_image_upload_click,
                    style=ft.ButtonStyle(
                        shape=ft.RoundedRectangleBorder(radius=8),
                        padding=ft.padding.symmetric(horizontal=20, vertical=10),
                    ),
                ),
                image_search_time_label,
            ],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=15,
        ),
        padding=20,
        border_radius=10,
        bgcolor=ft.Colors.WHITE70,
        shadow=ft.BoxShadow(spread_radius=1, blur_radius=5, color=ft.Colors.BLACK12, offset=ft.Offset(0, 2)),
    )

    page.add(
        ft.Container(
            content=ft.Row(
                [
                    ft.Column(
                        [
                            text_search_section,
                            image_search_section,
                        ],
                        expand=2,
                        spacing=20,
                        scroll=ft.ScrollMode.ADAPTIVE,
                    ),
                    ft.VerticalDivider(width=1),
                    ft.Column(
                        [
                            ft.Container(
                                content=ft.Column(
                                    [
                                        ft.Text(
                                            "Ingresa el número K de productos que deseas encontrar:",
                                            size=16,
                                            weight=ft.FontWeight.BOLD,
                                        ),
                                        k_input,
                                    ],
                                    spacing=5,
                                    horizontal_alignment=ft.CrossAxisAlignment.START,
                                ),
                                margin=ft.margin.only(bottom=10),
                            ),
                            results_grid_container,
                        ],
                        expand=3,
                        spacing=10,
                    ),
                ],
                expand=True,
                vertical_alignment=ft.CrossAxisAlignment.STRETCH,
                spacing=20,
            ),
            padding=20,
        )
    )

    update_results_grid([], False)

if __name__ == "__main__":
    ft.app(target=main)