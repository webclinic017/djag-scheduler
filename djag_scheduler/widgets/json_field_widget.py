"""django_json_widget.widgets.JSONEditorWidget with custom template"""

from django_json_widget.widgets import JSONEditorWidget


class JSONFieldWidget(JSONEditorWidget):
    template_name = 'widgets/json_field_widget.html'
