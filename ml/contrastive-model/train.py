import torch
import numpy as np
from tqdm import tqdm
from torch import nn
from torch.utils.data import TensorDataset, DataLoader


class Model(nn.Module):
    def __init__(self, in_features, out_features):
        super().__init__()
        self.embedder = nn.Linear(in_features, out_features)
    def forward(self, x):
        x = self.embedder(x)
        return x / (x ** 2).sum() ** 0.5

def generate_negatives(positive):
    # chance of match ~ 0.0002%
    return torch.randint(0, len(nodes), positive.shape).to('cuda')

# load dataset
nodes = torch.tensor(np.load('author_embeddings.npy')).to('cuda').float()
positive = torch.tensor(np.load('edges.npy')).to('cuda').T

# split to train and test
positive = positive[torch.randperm(positive.size(0))]
train, test = positive[:int(positive.size(0) * 0.8)], positive[int(positive.size(0) * 0.8):]

# create model instance
model = Model(nodes.shape[-1], 128).to('cuda')

loss_fn = nn.MSELoss()
optim = torch.optim.Adam(model.parameters())
best = 0

# train cycle
batch_size = 128

positive_train = TensorDataset(train)
positive_train = DataLoader(positive_train, batch_size=batch_size, shuffle=True)

positive_test = TensorDataset(test)
positive_test = DataLoader(positive_test, batch_size=batch_size, shuffle=True)

for epoch in range(30):
    negative_train = TensorDataset(generate_negatives(train))
    negative_train = DataLoader(negative_train, batch_size=batch_size, shuffle=True)
    for pos, neg in tqdm(zip(positive_train, negative_train)):
        inp = torch.cat((pos[0], neg[0]))
        out = torch.cat((torch.ones(batch_size) * 0.8, torch.ones(batch_size) * 0.2)).to('cuda')

        idx = torch.randperm(inp.size(0))
        inp = inp[idx]
        out = out[idx]

        a, b = nodes[inp[:, 0]], nodes[inp[:, 1]]
        a, b = model(a), model(b)

        cos = ((a * b).sum(axis=-1) + 1) / 2

        loss = loss_fn(cos, out)

        optim.zero_grad()
        loss.backward()
        optim.step()

    negative_test = TensorDataset(generate_negatives(test))
    negative_test = DataLoader(negative_test, batch_size=batch_size, shuffle=True)
    acc = [0, 0]
    for pos, neg in tqdm(zip(positive_test, negative_test)):
        inp = torch.cat((pos[0], neg[0]))
        out = torch.cat((torch.ones(batch_size), torch.zeros(batch_size))).to('cuda')

        idx = torch.randperm(inp.size(0))
        inp = inp[idx]
        out = out[idx]

        a, b = nodes[inp[:, 0]], nodes[inp[:, 1]]
        a, b = model(a), model(b)

        cos = ((a * b).sum(axis=-1) + 1) / 2

        acc[0] += ((cos > 0.5).float() == out).sum()
        acc[1] += len(out)
    acc = acc[0] / acc[1]
    # update best
    if acc > best:
        best = acc
        torch.save(model.state_dict(), 'model_weights.pth')
    print(f'Accuracy: {acc}')