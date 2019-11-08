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
import os, sys, argparse, configparser, traceback, base64

import tkinter as tk
from tkinter import font, ttk, scrolledtext, filedialog

# Local imports
from herdcl import logger, otags

LOGGER = logger.get_logger(__name__)
ALL = tk.N + tk.S + tk.E + tk.W
ACTIONS = ["Tags", "Objects", "Columns", "Samples", "BDef Description"]
ENVS = ["DEV", "DEV-INT", "QA", "QA-INT", "QA-STRESS", "PROD", "PROD-CT"]


################################################################################
class Application(tk.Frame):
    """
     The GUI Application class.
    """

    names = None
    fileName = ""
    env = None
    textPad = None
    wb = None
    gui_enabled = True

    def __init__(self, master=None):
        """
        The init method for the Application class.

        :param master: the parent widget
        """

        self.env_name = ENVS[0]
        self.action = ACTIONS[0]

        tk.Frame.__init__(self, master)
        tk.font.nametofont("TkDefaultFont").configure(size=10)
        tk.font.nametofont("TkTextFont").configure(size=11)
        tk.font.nametofont("TkFixedFont").configure(size=11)

        self.option_add('*Dialog.msg.font', 'Helvetica 14')

        self.username = tk.StringVar()
        self.userpwd = tk.StringVar()
        self.delete = tk.BooleanVar()
        self.grid(sticky=ALL)

        self.act = {0: self.test_api,
                    1: self.test_api,
                    2: self.test_api,
                    3: self.test_api,
                    4: self.test_api}
        # TODO change act object

        self.path, self.config = self.load_config()
        self.load_console_vars()
        self.create_widgets()

    ############################################################################
    @staticmethod
    def load_config():
        """
        Load configuration file

        :return: current working directory and configparser
        """

        LOGGER.debug(sys.argv)
        if os.name == 'nt':
            path = os.getcwd()
        elif '/' in sys.argv[0]:
            path = '/'.join(sys.argv[0].split('/')[:-1])
        else:
            path = os.getcwd()
        LOGGER.debug('Current working directory: {}'.format(path))

        # Get config file
        config_file = path + "/loader.cfg"
        LOGGER.debug('Checking for loader config: {}'.format(config_file))
        if not os.path.exists(config_file):
            message = "No config file found"
            LOGGER.error(message)
            raise Exception(message)
        config = configparser.ConfigParser()
        config.read(config_file)

        return path, config

    ############################################################################
    def create_widgets(self):
        """
        Create forms on GUI
        """

        top = self.winfo_toplevel()
        top.rowconfigure(0, weight=1)
        top.columnconfigure(0, weight=1)

        self.rowconfigure(2, weight=1)
        self.columnconfigure(2, weight=1)

        ######## row 0

        user_frame = tk.ttk.Labelframe(self, text="UserId and Password")
        user_frame.grid(row=0, column=0, columnspan=2, sticky=ALL)

        username_entry = tk.ttk.Entry(user_frame, width=22, textvariable=self.username)
        username_entry.grid(row=0, column=0, pady=5, padx=5, sticky=ALL)
        userpwd_entry = tk.ttk.Entry(user_frame, width=22, show="*", textvariable=self.userpwd)
        userpwd_entry.grid(row=0, column=1, pady=5, padx=5, sticky=ALL)

        ######## row 1

        env = tk.ttk.Labelframe(self, text='Environment')
        self.env = tk.ttk.Combobox(env, values=ENVS, state='readonly', width=11)
        self.env.bind("<<ComboboxSelected>>", self.select_env)
        self.env.current(0)  # set selection
        self.env.grid(row=0, pady=5, padx=5, sticky=ALL)
        env.grid(row=1, column=0, sticky=ALL)

        names = tk.ttk.Labelframe(self, text='Action')
        self.names = tk.ttk.Combobox(names, values=ACTIONS, state='readonly', width=11)
        self.names.bind("<<ComboboxSelected>>", self.select_action)
        self.names.current(0)  # set selection
        self.names.grid(row=0, pady=5, padx=5, sticky=ALL)
        names.grid(row=1, column=1, sticky=ALL)

        runs = tk.ttk.Labelframe(self, text='Go')
        lb = tk.ttk.Button(runs, text="Run", command=self.run)
        lb.grid(row=0, pady=5, padx=5, sticky=ALL)
        runs.grid(row=1, column=3)  # , sticky=S)

        ######## row 2

        self.textPad = tk.scrolledtext.ScrolledText(self,
                                                    inactiveselectbackground="grey")
        self.textPad.grid(row=2, column=0, columnspan=4, sticky=ALL)
        self.textPad.tag_configure("search", background="green")
        self.textPad.tag_configure("error", foreground="red")
        self.textPad.bind_class("Text", "<Control-a>", lambda event: event.widget.tag_add("sel", "1.0", "end"))

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

        if "ERROR" in output:
            self.textPad.insert(tk.END, output, "error")
        else:
            self.textPad.insert(tk.END, output)
        self.textPad.see(tk.END)
        self.textPad.update()

    ############################################################################
    def select_env(self, *args):
        """
        Gets env when user clicks on env dropdown
        """
        self.env_name = str(self.env.get())

    ############################################################################
    def select_action(self, *args):
        """
        Gets action when user clicks on action dropdown
        """
        self.action = str(self.names.get())

    ############################################################################
    def run(self):
        """
        Runs program when user clicks Run button
        """
        self.textPad.delete('1.0', tk.END)

        creds = {
            'url': self.config.get('url', self.env_name),
            'userName': self.username.get(),
            'userPwd': self.userpwd.get()
        }
        otags.setup_config(creds)

        try:
            self.act[ACTIONS.index(self.action)]()
        except Exception:
            print(traceback.format_exc())

        # TODO Run Summary
        self.display("\n-- RUN COMPLETED ---")

    ############################################################################
    def test_api(self):
        resp = otags.get_build_info()
        self.display(resp)

    ############################################################################
    def display(self, resp):
        LOGGER.info(resp)
        if self.gui_enabled:
            self.line(resp)

    ############################################################################
    def get_config_value(self, section, key):
        try:
            return self.config.get(section, key)
        except configparser.NoOptionError:
            LOGGER.warning('Key: {} not found in config file'.format(key))
            return None

    ############################################################################
    def load_console_vars(self):
        self.username.set(self.get_config_value('credentials', 'userName'))
        self.userpwd.set(base64.b64decode(self.get_config_value('credentials', 'userPwd')).decode('utf-8'))


############################################################################
def main():
    app = Application()

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--console", help="Command Line Mode", action="store_true")
    args = parser.parse_args()
    if args.console:
        app.gui_enabled = False
        LOGGER.info('Command Line Mode')
        app.run()
    else:
        LOGGER.info('Starting App')
        app.master.title('Herd Content Loader  v.20191029')
        app.mainloop()


################################################################################
if __name__ == "__main__":
    main()
