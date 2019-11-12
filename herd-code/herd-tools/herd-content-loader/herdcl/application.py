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
import argparse, base64

# Local imports
from herdcl import logger, otags

LOGGER = logger.get_logger(__name__)

################################################################################
class Application:
    """
     The application class. Main class
    """
    def __init__(self):
        self.gui_enabled = False
        self.controller = otags.Controller()
        self.path, self.config = self.controller.load_config()
        self.env = self.config.get('console', 'env')
        self.action = self.config.get('console', 'action')

    ############################################################################
    def run(self):
        """
        Runs program by loading credentials and making call to controller
        """
        creds = {
            'url': self.config.get('url', self.env),
            'userName': self.config.get('credentials', 'userName'),
            'userPwd': base64.b64decode(self.config.get('credentials', 'userPwd')).decode('utf-8')
        }
        self.controller.setup_config(creds)

        resp = self.controller.run_action(str.lower(self.action))
        LOGGER.info(resp)

        LOGGER.info("\n-- RUN COMPLETED ---")

############################################################################
def main():
    """
     The main method. Checks if argument has been passed to determine console mode or gui mode
    """
    main_app = Application()
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--console", help="Command Line Mode", action="store_true")
    args = parser.parse_args()
    if args.console:
        LOGGER.info('Command Line Mode')
        main_app.run()
    else:
        main_app.gui_enabled = True
        import gui
        app = gui.MainUI()
        LOGGER.info('Starting App')
        app.master.title('Herd Content Loader  v.20191029')
        app.mainloop()


################################################################################
if __name__ == "__main__":
    main()
