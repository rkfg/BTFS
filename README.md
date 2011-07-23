# What the? #
It's bittorent file system. It allows you to watch video or pictures, listen to music and so on directly from torrent network. The file you request is put on top of download queue so you shouldn't mess with priorities and such. Just mount this nifty filesystem wherever you like and use any player to get the content. It may get a while to fill the player's cache so be patient. If your connection is fast enough you'll expirience a smooth playback.

# A man page? #
There's no man page yet. I don't know whether it will be at all. For now you can use the following switches:

    -f torrent — path to the torrent file (required)
    -s save/to/dir — path to download dir (required)
    -p port — tcp (for torrent) and udp (for DHT) port to listen to
    -r path/to/resume.dat — use the resume file for faster incomplete download resuming
    --piece-par N — download N pieces in parallel
    --log — enable logging (provide a python logging configuration on /etc/btfs/logging.conf )
    --log-conf path/to/conf — use another logging configuration file

And after all switches put the path to the mount point. For example:

    ./btfs.py -f ~/mytorrent.torrent -s ~/downloads ~/btfs

Then just go to ~/btfs and look for files. The topmost directory will have the torrent name (I believe some day I'll add multiple torrents support so it's a good reason to separate them in advance). You can unmount it just like any other FUSE filesystem using `fusermount -u ~/btfs`.

# Bugs? #
Yeah, there may be some. I didn't make this program from scratch. Instead, I got bittorrent2player ( http://www.beroal.in.ua/prg/bittorrent2player/ ) and modified it a lot. So if there were bugs they are now here. And some authentic ones were introduced by me. Enjoy!

P.S. if you can't enjoy bugs for some reason feel free to file a ticket using github issue tracker. I'll try to fight them.
