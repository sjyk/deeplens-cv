from __future__ import unicode_literals

def youtube_fetch(ids, tmpfolder='/tmp/'):
	import youtube_dl

	url = 'https://www.youtube.com/watch?v='+ids

	ydl_opts = {'outtmpl': tmpfolder+'/%(id)s.%(ext)s', 'format': 'mp4'}
	with youtube_dl.YoutubeDL(ydl_opts) as ydl:
		ydl.download([url])

	return tmpfolder + ids + '.mp4'