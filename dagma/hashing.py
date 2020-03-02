HASH_BLOCK_SIZE = 1024 * 64


def hash_bytes(byte_blocks, hasher):
    for block in byte_blocks:
        hasher.update(block)
    return hasher.hexdigest()


def file_as_blocks(file_path, block_size=HASH_BLOCK_SIZE):
    with open(file_path, "rb") as f_in:
        block = f_in.read(block_size)
        while len(block) > 0:
            yield block
            block = f_in.read(block_size)
