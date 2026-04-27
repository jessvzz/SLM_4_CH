import torch
from unsloth import FastLanguageModel
from datasets import load_dataset
from trl import SFTTrainer
from transformers import TrainingArguments, DataCollatorForSeq2Seq
import wandb  

# CONFIG

MODEL_NAME = "unsloth/Qwen2.5-7B-bnb-4bit"  # quantized 4-bit
MAX_SEQ_LENGTH = 2048 # max seq length to configure
DTYPE = None # auto-detect (bfloat16 on Ada)
LOAD_IN_4BIT = True

# lora
LORA_R = 16
LORA_ALPHA = 32
LORA_DROPOUT = 0.05
TARGET_MODULES = [
    "q_proj", "k_proj", "v_proj", "o_proj",
    "gate_proj", "up_proj", "down_proj"
]

# training params

OUTPUT_DIR = "./cpt_output"
DATASET_PATH = "dataset.jsonl"
NUM_EPOCHS = 1
BATCH_SIZE = 2 # bc we have 20GB VRAM
GRAD_ACCUM = 8 # effective batch = 16
WARMUP_STEPS = 100
MAX_STEPS = -1
LR_MAX = 1e-4
LR_MIN = 1e-5


# LOAD MODEL

print("Loading model...")
model, tokenizer = FastLanguageModel.from_pretrained(
    model_name=MODEL_NAME,
    max_seq_length=MAX_SEQ_LENGTH,
    dtype=DTYPE,
    load_in_4bit=LOAD_IN_4BIT,
)

# LORA ADAPTER
model = FastLanguageModel.get_peft_model(
    model,
    r=LORA_R,
    target_modules=TARGET_MODULES,
    lora_alpha=LORA_ALPHA,
    lora_dropout=LORA_DROPOUT,
    bias="none",
    use_gradient_checkpointing="unsloth",  # to save VRAM
    random_state=42,
)

print(model.print_trainable_parameters())



# DATASET

def load_jsonl(path):
    import json
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def format_record(record):
    country = record.get("country", "unknown")
    title = record.get("title", "")
    
    description = record.get("description") or record.get("text", "")

    if country not in ("unknown", "Europe", None, ""):
        header = f"{title} | {country}"
    else:
        header = title

    text = f"{header}\n\n{description}"

    return {"text": text}

print("Loading dataset...")
raw_records = load_jsonl(DATASET_PATH)
formatted = [format_record(r) for r in raw_records]

# convertinh in HuggingFace Dataset
from datasets import Dataset
train_dataset = Dataset.from_list(formatted)


print(f"Train: {len(train_dataset)} record")


# TRAINING ARGUMENTS
training_args = SFTConfig(
    output_dir=OUTPUT_DIR,
    num_train_epochs=NUM_EPOCHS,
    per_device_train_batch_size=BATCH_SIZE,
    gradient_accumulation_steps=GRAD_ACCUM,

    # LR schedule: warmup + cosine decay
    learning_rate=LR_MAX,
    lr_scheduler_type="cosine",
    warmup_steps=WARMUP_STEPS,

    # stability
    max_grad_norm=1.0, # gradient clipping
    weight_decay=0.01,

    # precision (depends on gpu)
    fp16=not torch.cuda.is_bf16_supported(),
    bf16=torch.cuda.is_bf16_supported(),

    # Logging
    logging_steps=50,
    eval_strategy="no",
    save_strategy="steps",
    save_steps=500,
    save_total_limit=3, #saves last three checkpoints
    load_best_model_at_end=False,

    gradient_checkpointing=True,
    optim="adamw_8bit", #optimizer 8bit to save VRAM

    # Seed
    seed=42,

    # Report
    report_to="wandb", #to moinitor training

    # SFT-specific params (moved from SFTTrainer)
    dataset_text_field="text",
    max_length=MAX_SEQ_LENGTH,
    dataset_num_proc=2,
    packing=True,
)


# TRAINER

trainer = SFTTrainer(
    model=model,
    processing_class=tokenizer,
    train_dataset=train_dataset,
    args=training_args,
)


# TRAINING

print("Start training...")
print(f"  GPU: {torch.cuda.get_device_name(0)}")
print(f"  VRAM available: {torch.cuda.get_device_properties(0).total_memory / 1e9:.1f} GB")

trainer_stats = trainer.train()

print(f"\nTraining compelte")
print(f"  Time: {trainer_stats.metrics['train_runtime']:.0f}s")
print(f"  Final loss: {trainer_stats.metrics['train_loss']:.4f}")


print("Saving model...")
model.save_pretrained(f"{OUTPUT_DIR}/final")
tokenizer.save_pretrained(f"{OUTPUT_DIR}/final")

# merged model (base + lora) for inference
model.save_pretrained_merged(f"{OUTPUT_DIR}/merged", tokenizer)

print("Done!")