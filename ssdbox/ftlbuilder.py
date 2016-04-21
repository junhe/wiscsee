import config
import flash
import recorder

class FtlBuilder(object):
    def __init__(self, confobj, recorderobj, flashobj):
        if not isinstance(confobj, config.Config):
            raise TypeError('confobj is not of type config.Config, it is {}'.
               format(type(confobj).__name__))
        if not isinstance(recorderobj, recorder.Recorder):
            raise TypeError('recorder is not of type recorder.Recorder, "\
                "it is{}'.format(type(recorderobj).__name__))
        if not isinstance(flashobj, flash.Flash):
            raise TypeError('flash is not of type flash.Flash'.
               format(type(flashobj).__name__))

        self.conf = confobj
        self.recorder = recorderobj
        self.flash = flashobj

        if self.conf['workload_src'] == config.LBAGENERATOR:
            self.recorder.enable()
        elif self.conf['workload_src'] == config.WLRUNNER:
            self.recorder.disable()
        else:
            raise RuntimeError("workload_src:{} is not supported".format(
                self.conf['workload_src']))

    def lba_read(self, page_num):
        raise NotImplementedError

    def lba_write(self, page_num):
        raise NotImplementedError

    def lba_discard(self, page_num):
        raise NotImplementedError

    def sec_read(self, sector, count):
        raise NotImplementedError

    def sec_write(self, sector, count, data):
        raise NotImplementedError

    def sec_discard(self, sector, count):
        raise NotImplementedError

    def debug_info(self):
        raise NotImplementedError

    def enable_recording(self):
        self.recorder.enable()

    def disable_recording(self):
        self.recorder.disable()

    def pre_workload(self):
        """
        This will be called right before workload to be tested.
        It is after mounting and aging.
        """
        raise NotImplementedError

    def post_processing(self):
        raise NotImplementedError

    def get_type(self):
        return "FtlBuilder"


