import progressbar

class DownloadProgressBar:

    def __init__(self):
        self.progress_bar = None

    def __call__(self, block_num, block_size, total_size):

        if not self.progress_bar:
            self.progress_bar = progressbar.ProgressBar(maxval=total_size)
            self.progress_bar.start()

        downloaded = block_num * block_size
        if downloaded < total_size:
            self.progress_bar.update(downloaded)
        else:
            self.progress_bar.finish()
