"""Periodic Task Forms"""

from celery import current_app
from celery.utils import cached_property
from django import forms
from django.forms.widgets import Select, TextInput

from djag_scheduler.models import PeriodicTask


class TaskSelectWidget(Select):
    """Widget that lets you choose between task names."""

    celery_app = current_app
    _choices = None

    def tasks_as_choices(self):
        _ = self._modules  # noqa
        tasks = list(sorted(name for name in self.celery_app.tasks
                            if not name.startswith('celery.')))
        return (('', ''),) + tuple(zip(tasks, tasks))

    @property
    def choices(self):
        if self._choices is None:
            self._choices = self.tasks_as_choices()
        return self._choices

    @choices.setter
    def choices(self, _):
        # ChoiceField.__init__ sets ``self.choices = choices``
        # which would override ours.
        pass

    @cached_property
    def _modules(self):
        self.celery_app.loader.import_default_modules()
        return None


class TaskChoiceField(forms.ChoiceField):
    """Field that lets you choose between task names."""

    widget = TaskSelectWidget

    def valid_value(self, value):   # noqa
        return True


class ExceptionCronWidget(TextInput):
    """Widget for exception cron"""
    template_name = "widgets/exception_cron_widget.html"


class PeriodicTaskForm(forms.ModelForm):
    """Form that lets you create and modify periodic tasks."""

    task = TaskChoiceField(
        label='Task (registered)'
    )
    exception_cron = forms.DateTimeField(
        widget=ExceptionCronWidget, required=False
    )

    class Meta:
        """Form metadata."""

        model = PeriodicTask
        exclude = ()

    def clean(self):
        """Ensure exception cron is only cleared but not altered"""
        cleaned_data = super().clean()
        new_exception_cron = cleaned_data.get('exception_cron')

        if new_exception_cron is not None and new_exception_cron != self.instance.exception_cron:
            self.add_error(
                'exception_cron', 'Exception cron can only be cleared'
            )

        return cleaned_data
