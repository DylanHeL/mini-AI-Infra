import hashlib
from sequence import *

class Block:
    def __init__(self, block_id):
        self.block_id = block_id
        self.token_ids = []
        self.ref_cache = 0
        self.block_hash = None

    def __len__(self):
        return len(self.token_ids)

    def __iter__(self):
        return iter(self.token_ids)

    def __getitem__(self, k):
        return self.token_ids[k]

    def free(self):
        self.ref_cache = 0
        self.token_ids = []
        self.block_hash = None

class BlockManager:
    def __init__(self, block_size, num_blocks):
        self.block_size = block_size
        self.num_blocks = num_blocks
        self.blocks: list[Block] = [Block(i) for i in range(self.num_blocks)]
        self.hash_to_block = dict()
        self.free_blocks = list(range(num_blocks))

    def box_block(self, sequence:Sequence, block_idx):
        assert len(sequence) >= self.block_size * (block_idx+1)
        token_ids = sequence.tokens_in_block(block_idx)

        if block_idx == 0:
            data = str(tuple(token_ids)).encode('utf-8')
            hash = hashlib.sha256(data).hexdigest()
        else:
            pre_hash = self.blocks[sequence.block_list[block_idx-1]].block_hash
            data = (pre_hash+":"+str(tuple(token_ids))).encode('utf-8')
            hash = hashlib.sha256(data).hexdigest()
        if hash in self.hash_to_block:
            self.blocks[self.hash_to_block[hash]].ref_cache += 1
            self.blocks[sequence.block_list[block_idx]].free()
            self.free_blocks.append(sequence.block_list[block_idx])
            sequence.block_list[block_idx] = self.hash_to_block[hash]
        else:
            self.hash_to_block[hash] = sequence.block_list[block_idx]
            self.blocks[sequence.block_list[block_idx]].ref_cache += 1
            self.blocks[sequence.block_list[block_idx]].block_hash = hash


    def allocate_for_sequence(self, sequence:Sequence):
        assert not sequence.block_list
        full_load = 1 if len(sequence)%self.block_size==0 else 0
        blocks_needed = len(sequence)//self.block_size +1-full_load
        if len(self.free_blocks)<blocks_needed:
            return False
        else:
            sequence.num_blocks = blocks_needed
            for i in range(blocks_needed):
                new_block_id = self.free_blocks.pop()
                self.blocks[new_block_id].token_ids = list(sequence.tokens_in_block(i))
                sequence.block_list.append(new_block_id)
                if full_load or i != blocks_needed - 1:
                    self.box_block(sequence, i)
            return True

    def deallocate_for_sequence(self, sequence:Sequence):
        block_freed = 0
        for block_id in sequence.block_list:
            assert self.blocks[block_id].ref_cache > 0, f"Block {block_id} ref_cache is {self.blocks[block_id].ref_cache}, cannot decrement"
            self.blocks[block_id].ref_cache -= 1
            if self.blocks[block_id].ref_cache == 0:
                block_freed += 1
                if self.blocks[block_id].block_hash is not None and self.blocks[block_id].block_hash in self.hash_to_block:
                    del self.hash_to_block[self.blocks[block_id].block_hash]
                self.blocks[block_id].free()
                self.free_blocks.append(block_id)
        sequence.reset()
        return block_freed

    def manage_blocks_after_append(self, sequence:Sequence):
        # after sequence.append_token() manage block allocate
        if len(sequence) % self.block_size == 0:
            self.box_block(sequence, sequence.num_blocks-1)
        elif len(sequence) % self.block_size == 1:
            if len(self.free_blocks) == 0:
                return False
            else:
                new_block_id = self.free_blocks.pop()
                sequence.num_blocks+=1
                sequence.block_list.append(new_block_id)
        else:
            pass
        return True













