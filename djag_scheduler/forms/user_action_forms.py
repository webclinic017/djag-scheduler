"""User-Action Forms"""

from django import forms
from django.core.exceptions import SuspiciousOperation

import djag_scheduler.models.user_action_model as action_choices
from djag_scheduler.models import (
    UserAction
)
from djag_scheduler.widgets import JSONFieldWidget


class UserActionSelectForm(forms.Form):
    """Form for selecting user action"""

    action = forms.ChoiceField(
        label='User Action',
        choices=(('', '-------'),) + action_choices.PUBLIC_ACTION_CHOICES,
        label_suffix=':  '
    )


class BaseUserActionChangeForm(forms.ModelForm):
    """Base Form that takes action payload"""

    class Meta:
        model = UserAction
        fields = ('action',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields.get('action').disabled = True

        self.set_init_values()

    def set_init_values(self):
        """Set initial field values from action payload"""
        if self.instance is not None:
            for key, value in self.instance.payload.items():
                self.fields[key].initial = value

    def save(self, commit=True):
        """Save user-action: Construct payload from form fields"""
        user_action = super().save(commit=False)
        self.set_payload(user_action)

        if commit:
            user_action.save()

        return user_action

    def set_payload(self, user_action):
        """Set user_action.payload (Don't call .save() on user_action)"""

        payload = {}
        for key, value in self.cleaned_data.items():
            if key == 'action':
                continue

            if isinstance(value, (int, str, float, bool, list, tuple, dict, type(None))):
                payload[key] = value
            else:
                raise NotImplementedError('Saving value of type {0} is not implemented'.format(type(value)))

        user_action.payload = payload

    def clean_action(self):
        if (self.cleaned_data.get('action') not in
                [action_id for action_id, _ in action_choices.PUBLIC_ACTION_CHOICES]):
            raise SuspiciousOperation('Action is prohibited')

        return self.cleaned_data.get('action')


class GenericUserActionChangeForm(BaseUserActionChangeForm):
    """Generic User Action Change Form"""

    class Meta(BaseUserActionChangeForm.Meta):
        fields = BaseUserActionChangeForm.Meta.fields + ('payload',)
        widgets = {
            'payload': JSONFieldWidget
        }

    def set_init_values(self):
        pass

    def save(self, commit=True):
        return super(BaseUserActionChangeForm, self).save(commit)


class GenericUserActionReadForm(BaseUserActionChangeForm):
    """Generic User Action Change Form"""

    class Meta(BaseUserActionChangeForm.Meta):
        fields = BaseUserActionChangeForm.Meta.fields + ('payload',)
        widgets = {
            'payload': JSONFieldWidget(options={
                'mode': 'view',
                'modes': ['view']
            }),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields.get('payload').disabled = True

    def set_init_values(self):
        return

    def set_payload(self, user_action):
        return

    def clean_action(self):
        return self.cleaned_data.get('action')


def get_action_form(action):
    """Get form based on action"""

    if action in [action_id for action_id, _ in
                  action_choices.INTERNAL_ACTION_CHOICES]:
        return GenericUserActionReadForm
    else:
        return GenericUserActionChangeForm
