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
import argparse, json, traceback

# Local imports
try:
    import logger, otags
except ImportError:
    from herdcl import logger, otags

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
            self.controller.setup_run(config)
            method = self.controller.get_action()
            resp = self.controller.get_build_info()
            LOGGER.info(resp)
            run_summary = method()

            LOGGER.info('\n\n--- RUN SUMMARY ---')
            LOGGER.info('Processed {} rows'.format(run_summary['total_rows']))
            LOGGER.info('Number of rows succeeded: {}'.format(run_summary['success_rows']))
            if len(run_summary['warnings']) > 0:
                LOGGER.info('\n--- RUN WARNINGS ---')
                for e in run_summary['warnings']:
                    LOGGER.warning('Row: {}\nMessage: {}'.format(e['index'], e['message']))
            if run_summary['fail_rows'] == 0:
                LOGGER.info('\n--- RUN COMPLETED ---')
            else:
                LOGGER.error('\n--- RUN FAILURES ---')
                LOGGER.error('Number of rows failed: {}'.format(run_summary['fail_rows']))
                LOGGER.error('Please check rows: {}\n'.format(run_summary['fail_index']))
                for e in run_summary['errors']:
                    LOGGER.error('Row: {}\nMessage: {}'.format(e['index'], e['message']))
                LOGGER.error('\n--- RUN COMPLETED WITH FAILURES ---')
        except Exception:
            LOGGER.error(traceback.print_exc())
            LOGGER.error('\n--- RUN COMPLETED WITH FAILURES ---')


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
        main_app.controller.gui_enabled = True
        import gui
        app = gui.MainUI()
        LOGGER.info('Starting App')
        app.master.title('Herd Content Loader  v.20191112')
        app.mainloop()


################################################################################
if __name__ == "__main__":
    main()
