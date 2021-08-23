"""
  Copyright 2015 herd contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""
# Standard library imports
import time
import traceback

# Third party imports
import PySimpleGUI as sg

# Local imports
try:
    import logger, otags
    from constants import *
except ImportError:
    from herdcl import logger, otags
    from herdcl.constants import *

LOGGER = logger.get_logger(__name__)


################################################################################
class MainUI:
    """
     The GUI Application class.
    """

    names = None
    file_name = ""
    env = None
    text_pad = None
    window = None
    gui_enabled = True
    controller = otags.Controller()
    controller.load_config()

    def __init__(self):
        """
        The init method for the Application class.

        Set global style settings
        """
        sg.theme(DEFAULT_THEME)
        sg.theme_background_color(DEFAULT_COLOR)
        sg.theme_button_color(DEFAULT_BUTTON_COLOR)
        sg.SetOptions(font=DEFAULT_FONT)

    ############################################################################
    def get_layout(self):
        """
        Returns window layout of forms
        """
        layout_creds = [
            [get_input_box(key=WindowElement.USER.value), get_input_box_pwd(key=WindowElement.CRED.value)],
        ]

        layout_env = [
            [get_menu(items=self.controller.envs, key=WindowElement.ENV.value)],
        ]

        layout_switch = [
            [get_menu(items=['Import', 'Export'], key=WindowElement.SWITCH.value, event=True)],
        ]

        layout_action = [
            [get_menu(items=self.controller.actions, key=WindowElement.ACTION.value, event=True)],
        ]

        layout_export_action = [
            [get_menu(items=self.controller.export_actions, key=WindowElement.EXPORT_ACTION.value)],
        ]

        layout_excel = [
            [get_file(sg.user_settings_get_entry('-SaveExcel-', ''), key=WindowElement.EXCEL_FILE.value),
             sg.FileBrowse(size=(int(DEFAULT_WIDTH / 2), 1))],
        ]

        layout_samples = [
            [get_folder(sg.user_settings_get_entry('-SaveSample-', ''), key=WindowElement.SAMPLE_DIR.value),
             sg.FolderBrowse(size=(int(DEFAULT_WIDTH / 2), 1))],
        ]

        layout_namespace = [
            [get_input_box(key=WindowElement.NAMESPACE.value)],
        ]

        layout_export = [
            [get_folder(sg.user_settings_get_entry('-SaveExport-', ''), key=WindowElement.EXPORT_DIR.value),
             sg.FolderBrowse(size=(int(DEFAULT_WIDTH / 2), 1))],
        ]

        layout_bottom = [
            [get_checkbox('Debug Mode', key=WindowElement.DEBUG.value), get_button('Run')],
            [get_text_box(WindowElement.TEXTPAD.value)],
        ]

        row_1 = [
            [get_frame('UserId and Password', layout_creds)],
            [get_frame('Environment', layout_env), get_frame('Mode', layout_switch)],
        ]
        row_2a = [
            [get_frame('Metadata Type', layout_action)],
            [get_frame('Excel File', layout_excel)],
            [get_frame('Samples Directory', layout_samples, key='-SamplesFrame-', visible=False)],
        ]
        row_2b = [
            [get_frame('Metadata Type', layout_export_action)],
            [get_frame('Namespace', layout_namespace)],
            [get_frame('Export Directory', layout_export)],
        ]
        col = [
            [sg.Column(row_2a, key='-View1-'), sg.Column(row_2b, key='-View2-', visible=False)],
        ]
        row_3 = [
            [get_frame(None, layout_bottom)],
        ]

        layout = row_1 + col + row_3
        return layout

    ############################################################################
    def line(self, *args):
        """
        Outputs text and newline to GUI
        """
        output = ' '.join(str(a) for a in args)

        if not output:
            output = ''
        else:
            output = str(output)
        self.show(output + "\n")

    ############################################################################
    def show(self, *args):
        """
        Outputs text to GUI
        """
        output = ' '.join(str(a) for a in args)

        if not output:
            output = '\n'
        else:
            output = str(output)

        if "SUMMARY" in output:
            self.text_pad.insert("end", output, "black")
        elif "FAILURE" in output:
            self.text_pad.insert("end", output, "red")
        else:
            self.text_pad.insert("end", output)
        self.text_pad.see("end")

    ############################################################################
    def display(self, resp, log=None):
        """
        Displays message to both logger and GUI textPad
        """
        if log:
            log(resp)
        else:
            LOGGER.info(resp)
        self.line(resp)

    ############################################################################
    def run(self):
        """
        Runs program when user clicks Run button
        """
        self.window[WindowElement.TEXTPAD.value].update('')

        if not (self.window[WindowElement.USER.value].get() and self.window[WindowElement.CRED.value].get()):
            self.line("Enter credentials.")
            return

        action = self.window[WindowElement.ACTION.value].get()
        if self.window[WindowElement.SWITCH.value].get() == 'Import':
            if not self.window[WindowElement.EXCEL_FILE.value].get():
                self.line("Please select a file first.")
                return
            elif self.window[WindowElement.ACTION.value].get() == Menu.SAMPLES.value:
                if not self.window[WindowElement.SAMPLE_DIR.value].get():
                    self.line("Please select a directory.")
                    return
        else:
            action = self.window[WindowElement.EXPORT_ACTION.value].get()
            if not self.window[WindowElement.NAMESPACE.value].get():
                self.line("Enter namespace.")
                return

        config = {
            'gui_enabled': True,
            'debug_mode': self.window[WindowElement.DEBUG.value].get(),
            'env': self.window[WindowElement.ENV.value].get(),
            'action': action,
            'excelFile': self.window[WindowElement.EXCEL_FILE.value].get(),
            'sampleDir': self.window[WindowElement.SAMPLE_DIR.value].get(),
            'namespace': self.window[WindowElement.NAMESPACE.value].get(),
            'exportDir': self.window[WindowElement.EXPORT_DIR.value].get(),
            'userName': self.window[WindowElement.USER.value].get(),
            'userPwd': self.window[WindowElement.CRED.value].get()
        }

        try:
            self.controller.start_time = time.time()
            self.controller.setup_run(config)
            method = self.controller.get_action()
            self.display('Connection Check')
            self.controller.get_current_user()
            self.display('Success')
            self.display('Starting Run')
            run_summary = method()

            self.display('\n\n--- RUN SUMMARY ---')
            self.display('Run time: {} sec\n'.format(time.time() - self.controller.start_time))
            self.display('NOTES: {}\n'.format(run_summary[Summary.COMMENTS.value]))
            self.display('Processed {} rows'.format(run_summary[Summary.TOTAL.value]))
            self.display('Number of rows succeeded: {}'.format(run_summary[Summary.SUCCESS.value]))
            if len(run_summary[Summary.CHANGES.value]) > 0:
                changes = sorted(run_summary[Summary.CHANGES.value], key=lambda i: i[Summary.INDEX.value])
                self.display('\n--- RUN CHANGES ---')
                for e in changes:
                    self.display('Row: {}\nMessage: {}'.format(e[Summary.INDEX.value], e[Summary.MESSAGE.value]))
            if len(run_summary[Summary.WARNINGS.value]) > 0:
                warnings = sorted(run_summary[Summary.WARNINGS.value], key=lambda i: i[Summary.INDEX.value])
                self.display('\n--- RUN WARNINGS ---', log=LOGGER.warning)
                for e in warnings:
                    self.display('Row: {}\nMessage: {}'.format(e[Summary.INDEX.value], e[Summary.MESSAGE.value]),
                                 log=LOGGER.warning)
            if run_summary[Summary.FAIL.value] == 0:
                self.display('\n--- RUN COMPLETED ---')
            else:
                errors = sorted(run_summary[Summary.ERRORS.value], key=lambda i: i[Summary.INDEX.value])
                self.display('\n--- RUN FAILURES ---', log=LOGGER.error)
                self.display('Number of rows failed: {}'.format(run_summary[Summary.FAIL.value]), log=LOGGER.error)
                self.display('Please check rows: {}\n'.format(sorted(run_summary[Summary.FAIL_INDEX.value])),
                             log=LOGGER.error)
                for e in errors:
                    self.display('Row: {}\nMessage: {}'.format(e[Summary.INDEX.value], e[Summary.MESSAGE.value]),
                                 log=LOGGER.error)
                self.display('\n--- RUN COMPLETED WITH FAILURES ---', log=LOGGER.error)
        except Exception:
            self.display(traceback.print_exc(), log=LOGGER.error)
            self.display('\n--- RUN COMPLETED WITH FAILURES ---', log=LOGGER.error)

    ############################################################################
    def mainloop(self):
        """
        Create forms on GUI
        """
        self.window = sg.Window(DEFAULT_TITLE, layout=self.get_layout(), finalize=True)

        self.text_pad = self.window.FindElement(WindowElement.TEXTPAD.value).Widget
        self.text_pad.tag_configure("black", background="black", foreground="white")
        self.text_pad.tag_configure("red", foreground="red")

        while True:
            event, values = self.window.read()

            if event == sg.WIN_CLOSED:
                break
            elif event == WindowElement.SWITCH.value:
                if self.window[WindowElement.SWITCH.value].get() == 'Export':
                    self.window['-View1-'].update(visible=False)
                    self.window['-View2-'].update(visible=True)
                else:
                    self.window['-View1-'].update(visible=True)
                    self.window['-View2-'].update(visible=False)
            elif event == WindowElement.ACTION.value:
                if self.window[WindowElement.ACTION.value].get() == Menu.SAMPLES.value:
                    self.window['-SamplesFrame-'].update(visible=True)
                else:
                    self.window['-SamplesFrame-'].update(visible=False)

            elif event == 'Run':
                self.window['Run'].update(disabled=True)
                sg.user_settings_set_entry('-SaveExcel-', values[WindowElement.EXCEL_FILE.value])
                sg.user_settings_set_entry('-SaveSample-', values[WindowElement.SAMPLE_DIR.value])
                sg.user_settings_set_entry('-SaveExport-', values[WindowElement.EXPORT_DIR.value])

                self.run()
                self.window['Run'].update(disabled=False)

        self.window.close()


'''
############################################################################
--- Styling Section ---
############################################################################
'''

DEFAULT_TITLE = 'UDC Content Loader'
DEFAULT_THEME = 'DefaultNoMoreNagging'
DEFAULT_COLOR = '#F0F0F0'
DEFAULT_BUTTON_COLOR = '#007BFF'
DEFAULT_FONT = 'Helvetica 14'
DEFAULT_WIDTH = 30


def get_input_box(key):
    return sg.InputText(size=(DEFAULT_WIDTH, 1), key=key)


def get_input_box_double(key):
    return sg.InputText(size=(DEFAULT_WIDTH * 2, 1), key=key)


def get_input_box_pwd(key):
    return sg.InputText(size=(DEFAULT_WIDTH, 1), password_char='*', key=key)


def get_menu(items, key, event=False):
    return sg.Combo(items, size=(DEFAULT_WIDTH - 7, 1), default_value=items[0], key=key, enable_events=event)


def get_checkbox(text, key, event=False):
    return sg.Checkbox(text=text, key=key, enable_events=event, background_color=DEFAULT_COLOR)


def get_button(title):
    return sg.Button(title, size=(int(DEFAULT_WIDTH / 2), 1))


def get_file(save, key):
    return sg.Input(save, key=key)


def get_folder(save, key):
    return sg.Input(save, key=key)


def get_text_box(key):
    return sg.Multiline(size=(DEFAULT_WIDTH * 2 + 2, 20), key=key)


def get_frame(title, layout, key=None, visible=True):
    return sg.Frame(title, layout, border_width=0, key=key, visible=visible, background_color=DEFAULT_COLOR)
