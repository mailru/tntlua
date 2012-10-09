lua l = box.queue.put(0, 0, 'hello')
lua box.queue.delete(0, box.unpack('l', l[0]))
