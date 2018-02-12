try:
    import argparse, textwrap, os, sys, atexit, logging, logging.config

except ImportError as err:
    print('Error {0} import module: {1}'.format(__name__, err))
    exit(128)


class g2o_stops_commandline:
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog="g20-stops",
                                              formatter_class=argparse.RawTextHelpFormatter,
                                              description='',
                                              epilog=textwrap.dedent('''
        examples:
          g2o-stops.py --i ~/GTFS/bkk/ -o ./output_bkk/ -t node -v bus,tram,bkk

          g2o-stops.py --i ~/GTFS/mav/ -o ./output_mav/ -t node -v railway

        '''))

        self.parser.add_argument('-i', '--input', type=str, dest='input', required=True,
                                 help='required:  input folder')

        self.parser.add_argument('-o', '--output', type=str, dest='output', required=True,
                                 help='required:  output folder')

        self.parser.add_argument('-c', '--city', type=str, dest='city', required=False, default='Budapest',
                                 help='optional:  city (default: Budapest)')

        self.parser.add_argument('-t', '--type', type=str, dest='type', action='append', required=False,
                                 default=['node'],
                                 help='optional:  OSM object type')

        self.parser.add_argument('-v', '--vehicle', type=str, dest='vehicle', action='append', required=False,
                                 default=[],
                                 help='optional:  OSM vehicle type')

    def parse(self):
        self.args = self.parser.parse_args()

    @property
    def input(self):
        return self.args.input

    @property
    def output(self):
        return self.args.output

    @property
    def city(self):
        return self.args.city

    @property
    def type(self):
        return self.args.type

    @property
    def vehicle(self):
        if self.args.vehicle == []:
            return ['bus', 'tram', 'bkk']
        else:
            return self.args.vehicle
