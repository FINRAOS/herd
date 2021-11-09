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
import argparse
import traceback

# Local imports
try:
    import logger, otags
    from constants import *
except ImportError:
    from herdcl import logger, otags
    from herdcl.constants import *

LOGGER = logger.get_logger(__name__)


################################################################################
class Application:
    """
     The application class. Main class
    """

    def __init__(self):
        self.controller = otags.Controller()
        self.controller.load_config()

    ############################################################################
    def run(self):
        """
        Runs program by loading credentials and making call to controller
        """
        config = {
            'gui_enabled': False
        }

        try:
            self.controller.start_time = time.time()
            self.controller.setup_run(config)
            method = self.controller.get_action()
            LOGGER.info('Getting OAuth2 Token')
            self.controller.setup_access_token()
            LOGGER.info('Connection Check')
            self.controller.get_current_user()
            LOGGER.info('Success')
            run_summary = method()

            LOGGER.info('\n\n--- RUN SUMMARY ---')
            LOGGER.info('Run time: {} sec\n'.format(time.time() - self.controller.start_time))
            LOGGER.info('NOTES: {}\n'.format(run_summary[Summary.COMMENTS.value]))
            LOGGER.info('Processed {} rows'.format(run_summary[Summary.TOTAL.value]))
            LOGGER.info('Number of rows succeeded: {}'.format(run_summary[Summary.SUCCESS.value]))
            if len(run_summary[Summary.CHANGES.value]) > 0:
                changes = sorted(run_summary[Summary.CHANGES.value], key=lambda i: i[Summary.INDEX.value])
                LOGGER.info('\n--- RUN CHANGES ---')
                for e in changes:
                    LOGGER.info('Row: {}\nMessage: {}'.format(e[Summary.INDEX.value], e[Summary.MESSAGE.value]))
            if len(run_summary[Summary.WARNINGS.value]) > 0:
                warnings = sorted(run_summary[Summary.WARNINGS.value], key=lambda i: i[Summary.INDEX.value])
                LOGGER.info('\n--- RUN WARNINGS ---')
                for e in warnings:
                    LOGGER.warning('Row: {}\nMessage: {}'.format(e[Summary.INDEX.value], e[Summary.MESSAGE.value]))
            if run_summary[Summary.FAIL.value] == 0:
                LOGGER.info('\n--- RUN COMPLETED ---')
            else:
                errors = sorted(run_summary['errors'], key=lambda i: i[Summary.INDEX.value])
                LOGGER.error('\n--- RUN FAILURES ---')
                LOGGER.error('Number of rows failed: {}'.format(run_summary[Summary.FAIL.value]))
                LOGGER.error('Please check rows: {}\n'.format(sorted(run_summary[Summary.FAIL_INDEX.value])))
                for e in errors:
                    LOGGER.error('Row: {}\nMessage: {}'.format(e[Summary.INDEX.value], e[Summary.MESSAGE.value]))
                LOGGER.error('\n--- RUN COMPLETED WITH FAILURES ---')
        except Exception:
            LOGGER.error(traceback.print_exc())
            LOGGER.error('\n--- RUN COMPLETED WITH FAILURES ---')


############################################################################
def main():
    """
     The main method. Checks if argument has been passed to determine console mode or gui mode
    """
    LOGGER.info('Loading Application')
    main_app = Application()
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--console", help="Command Line Mode", action="store_true")
    args = parser.parse_args()
    if args.console:
        LOGGER.info('Command Line Mode')
        main_app.run()
    else:
        main_app.controller.gui_enabled = True
        try:
            import gui
        except ModuleNotFoundError:
            from herdcl import gui
        app = gui.MainUI()
        LOGGER.info('Opening GUI')
        app.mainloop()


################################################################################
if __name__ == "__main__":
    main()
