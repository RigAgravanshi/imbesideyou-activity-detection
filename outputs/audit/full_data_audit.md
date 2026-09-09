# Data Audit

Source type: `directory`

| Dataset | Roots | Sessions | Chunks | Events | GT records |
|---|---:|---:|---:|---:|---:|
| dataset_a | 1 | 63 | 133 | 162768 | 10609 |
| dataset_b | 1 | 15 | 20 | 20477 | 0 |

## Integrity signals

- Parse/read errors: **0**
- Structural warnings: **33**
- Duplicate archive member paths: **0**
- Duplicate logical files: **0**

## dataset_a

- Effective roots: dataset_a
- Operators / machines: 8 / 8
- Out-of-order events: 15231
- Duplicate event IDs / content: 0 / 0
- Session-ID mismatches: 0
- Events with extracted text: 7300
- Screenshot references: 34580
- Top event types: {'app_switch': 50588, 'keystroke': 38717, 'screenshot_smart': 34580, 'shortcut': 13668, 'mouse_click': 6177, 'browser_click': 5365, 'clipboard_change': 5198, 'mouse_scroll': 3180, 'browser_form_input': 1805, 'browser_navigation': 1725}
- Top applications: {'Google Chrome': 118134, 'Notepad': 30234, 'Microsoft Word': 5365, 'Microsoft PowerPoint': 3234, 'Windows Explorer': 1948, 'WindowsTerminal': 1687, 'Microsoft Excel': 887, 'procmine-desktop-agent': 320, 'olk': 182, 'Settings': 126}

## dataset_b

- Effective roots: dataset_b
- Operators / machines: 4 / 4
- Out-of-order events: 2922
- Duplicate event IDs / content: 0 / 0
- Session-ID mismatches: 0
- Events with extracted text: 939
- Screenshot references: 4759
- Top event types: {'screenshot_smart': 4759, 'keystroke': 4678, 'mouse_click': 2133, 'browser_click': 1914, 'mouse_scroll': 1724, 'app_switch': 1654, 'shortcut': 1386, 'clipboard_change': 872, 'browser_form_input': 608, 'browser_navigation': 204}
- Top applications: {'Microsoft Edge': 13300, 'Microsoft Word': 3704, 'Microsoft Excel': 1201, 'OpenWith': 757, 'Notepad': 599, 'WindowsTerminal': 407, 'procmine-desktop-agent': 205, 'Windows Explorer': 78, 'ms-teams': 73, 'prl_cc': 28}
