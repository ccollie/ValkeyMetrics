### VKM.LABEL_VALUES

#### Syntax

```
VM.LABEL-VALUES label [START fromTimestamp|rfc3339|+|*] [END toTimestamp|rfc3339|+|*]
```
returns a list of label values for a provided label name.

### Required Arguments

<details open><summary><code>label</code></summary>
The label name for which to retrieve values.
</details>

### Optional Arguments

<details open><summary><code>fromTimestamp</code></summary>
If specified along with `toTimestamp`, this limits the result to only labels from series which
have data in the date range [`fromTimestamp` .. `toTimestamp`]
</details>

<details open><summary><code>toTimestamp</code></summary>
If specified along with `fromTimestamp`, this limits the result to only labels from series which
have data in the date range [`fromTimestamp` .. `toTimestamp`]
</details>

#### Return

The data section of the JSON response is a list of string label values.

#### Error

Return an error reply in the following cases:

- Invalid options.
- TODO.

#### Examples

This example queries for all label values for the job label:
```
// Create a chat application with LLM model and vector store.
VKM.LABEL-VALUES job
```
```json
{
   "status" : "success",
   "data" : [
      "node",
      "prometheus"
   ]
}
```