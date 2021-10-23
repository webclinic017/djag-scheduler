"""User-Action Audit Forms"""

from django import forms

from djag_scheduler.models import (
    UserActionAudit
)
from djag_scheduler.widgets import JSONFieldWidget


class UserActionAuditPayloadForm(forms.ModelForm):
    """Form for viewing user-action audit's payload"""

    class Meta:
        model = UserActionAudit
        fields = ('payload',)
        widgets = {
            'payload': JSONFieldWidget(options={
                'mode': 'view',
                'modes': ['view']
            }),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields.get('payload').disabled = True
