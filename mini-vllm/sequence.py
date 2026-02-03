from enum import Enum, auto
from gc import freeze
from itertools import count


class SequenceStatus(Enum):
    RUNNING = auto()
    WAITING = auto()
    FINISHED = auto()

class Sequence:
    block_size = 256
    counter = count()

    def __init__(self, token_ids: list[int]):
        self.seq_id = next(Sequence.counter)
        self.status = SequenceStatus.WAITING
        self.token_ids = token_ids
        self.last_token = token_ids[-1] if token_ids else None
        self.num_tokens = len(token_ids)
        self.num_prompt_tokens = len(token_ids)
        self.num_cached_tokens = 0
        self.num_blocks = 0
        self.block_list = []

    def __len__(self):
        return self.num_tokens

    def __iter__(self):
        return iter(self.token_ids)

    def __getitem__(self, k):
        return self.token_ids[k]

    def __getstate__(self):
        return {
            'seq_id': self.seq_id, 'status': self.status, 'last_token': self.last_token,
            'num_tokens': self.num_tokens, 'num_prompt_tokens': self.num_prompt_tokens,
            'num_cached_tokens': self.num_cached_tokens,'block_list': self.block_list
        }
    def __setstate__(self, state):
        self.seq_id = state['seq_id']
        self.status = state['status']
        self.num_tokens = state['num_tokens']
        self.num_prompt_tokens = state['num_prompt_tokens']
        self.num_cached_tokens = state['num_cached_tokens']
        self.block_list = state['block_list']
        self.last_token = state['last_token']

    def tokens_in_block(self,block_idx):
        assert 0 <= block_idx < self.num_blocks
        if block_idx != self.num_blocks-1:
            return self.token_ids[block_idx*self.block_size:(block_idx+1)*self.block_size]
        else:
            return self.token_ids[block_idx*self.block_size:]

    def append_token(self, token_id):
        self.token_ids.append(token_id)
        self.last_token = token_id
        self.num_cached_tokens += 1

    def reset(self):
        self.block_list = []
        self.num_blocks = 0
        self.num_cached_tokens = 0
        self.status = SequenceStatus.WAITING

