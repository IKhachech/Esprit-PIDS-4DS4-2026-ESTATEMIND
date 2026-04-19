import numpy as np

img = np.load(r'C:\Users\user\Downloads\Esprit-PIDS-4DS4-2026-ESTATEMIND-main\Esprit-PIDS-4DS4-2026-ESTATEMIND-main\BO2\image_embeddings.npy')
txt = np.load(r'C:\Users\user\Downloads\Esprit-PIDS-4DS4-2026-ESTATEMIND-main\Esprit-PIDS-4DS4-2026-ESTATEMIND-main\BO2\text_embeddings.npy')

print("image_embeddings shape:", img.shape)
print("text_embeddings shape:", txt.shape)