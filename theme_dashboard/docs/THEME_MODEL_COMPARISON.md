# Theme Model Comparison

- Preferred source: `live`
- Run id: `7`
- Snapshot time: `2026-03-11 19:54:24.668853`

## Variant Definitions

- `baseline`: current production-style composite score using simple mean returns.
- `confidence_adjusted`: `baseline_score * min(1, sqrt(ticker_count / 8))`.
- `winsorized`: recompute average returns with 10/90 winsorized means when a theme has at least 5 valid members.
- `breadth_adjusted`: `baseline_score + 0.50 * ((breadth_1m - 50) / 10)`.
- `combined`: `(winsorized_score + 0.50 * breadth_signal) * confidence_factor`.

## Top 10 By Variant

### Baseline
| rank | theme | category | ticker_count | score | breadth_1m | top_abs_share_1m_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Edge Computing | Emerging Tech | 5 | 38.04 | 75.0 | 82.43 |
| 2 | Biofuels & Ethanol | Energy - Clean | 6 | 30.0 | 100.0 | 70.39 |
| 3 | Media - Publishing | Media & Entertainment | 5 | 24.0 | 100.0 | 61.18 |
| 4 | Cybersecurity - Network | Technology - Software | 6 | 22.02 | 50.0 | 78.52 |
| 5 | Chemicals - Plastics | Materials & Chemicals | 5 | 17.67 | 60.0 | 39.26 |
| 6 | Biotech - Oncology | Healthcare - Biotech | 10 | 17.14 | 55.56 | 47.36 |
| 7 | Aluminum | Metals & Mining | 4 | 15.28 | 66.67 | 44.13 |
| 8 | Oil - E&P | Energy - Oil & Gas | 12 | 13.88 | 100.0 | 18.22 |
| 9 | Space - Earth Observation | Space & Aerospace | 4 | 13.75 | 66.67 | 54.12 |
| 10 | Telehealth | Healthcare - Specialty | 5 | 13.73 | 80.0 | 28.15 |

### Confidence Adjusted
| rank | theme | category | ticker_count | score | breadth_1m | top_abs_share_1m_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Edge Computing | Emerging Tech | 5 | 30.08 | 75.0 | 82.43 |
| 2 | Biofuels & Ethanol | Energy - Clean | 6 | 25.98 | 100.0 | 70.39 |
| 3 | Cybersecurity - Network | Technology - Software | 6 | 19.07 | 50.0 | 78.52 |
| 4 | Media - Publishing | Media & Entertainment | 5 | 18.98 | 100.0 | 61.18 |
| 5 | Biotech - Oncology | Healthcare - Biotech | 10 | 17.14 | 55.56 | 47.36 |
| 6 | Chemicals - Plastics | Materials & Chemicals | 5 | 13.97 | 60.0 | 39.26 |
| 7 | Oil - E&P | Energy - Oil & Gas | 12 | 13.88 | 100.0 | 18.22 |
| 8 | Oil - Refiners | Energy - Oil & Gas | 9 | 13.32 | 100.0 | 17.04 |
| 9 | Oil - Offshore Drillers | Energy - Oil & Gas | 7 | 12.82 | 80.0 | 31.93 |
| 10 | Oil - Large Cap | Energy - Oil & Gas | 8 | 12.75 | 100.0 | 27.49 |

### Winsorized
| rank | theme | category | ticker_count | score | breadth_1m | top_abs_share_1m_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Edge Computing | Emerging Tech | 5 | 38.04 | 75.0 | 82.43 |
| 2 | Biofuels & Ethanol | Energy - Clean | 6 | 30.0 | 100.0 | 70.39 |
| 3 | Media - Publishing | Media & Entertainment | 5 | 24.0 | 100.0 | 61.18 |
| 4 | Chemicals - Plastics | Materials & Chemicals | 5 | 17.2 | 60.0 | 39.26 |
| 5 | Aluminum | Metals & Mining | 4 | 15.28 | 66.67 | 44.13 |
| 6 | Cybersecurity - Network | Technology - Software | 6 | 14.39 | 50.0 | 78.52 |
| 7 | Oil - E&P | Energy - Oil & Gas | 12 | 14.0 | 100.0 | 18.22 |
| 8 | Space - Earth Observation | Space & Aerospace | 4 | 13.75 | 66.67 | 54.12 |
| 9 | Oil - Refiners | Energy - Oil & Gas | 9 | 13.71 | 100.0 | 17.04 |
| 10 | Oil - Offshore Drillers | Energy - Oil & Gas | 7 | 13.62 | 80.0 | 31.93 |

### Breadth Adjusted
| rank | theme | category | ticker_count | score | breadth_1m | top_abs_share_1m_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Edge Computing | Emerging Tech | 5 | 39.3 | 75.0 | 82.43 |
| 2 | Biofuels & Ethanol | Energy - Clean | 6 | 32.5 | 100.0 | 70.39 |
| 3 | Media - Publishing | Media & Entertainment | 5 | 26.5 | 100.0 | 61.18 |
| 4 | Cybersecurity - Network | Technology - Software | 6 | 22.02 | 50.0 | 78.52 |
| 5 | Chemicals - Plastics | Materials & Chemicals | 5 | 18.17 | 60.0 | 39.26 |
| 6 | Biotech - Oncology | Healthcare - Biotech | 10 | 17.42 | 55.56 | 47.36 |
| 7 | Oil - E&P | Energy - Oil & Gas | 12 | 16.38 | 100.0 | 18.22 |
| 8 | Aluminum | Metals & Mining | 4 | 16.11 | 66.67 | 44.13 |
| 9 | Oil - Refiners | Energy - Oil & Gas | 9 | 15.82 | 100.0 | 17.04 |
| 10 | Oil - Large Cap | Energy - Oil & Gas | 8 | 15.25 | 100.0 | 27.49 |

### Combined
| rank | theme | category | ticker_count | score | breadth_1m | top_abs_share_1m_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Edge Computing | Emerging Tech | 5 | 31.07 | 75.0 | 82.43 |
| 2 | Biofuels & Ethanol | Energy - Clean | 6 | 28.15 | 100.0 | 70.39 |
| 3 | Media - Publishing | Media & Entertainment | 5 | 20.95 | 100.0 | 61.18 |
| 4 | Oil - E&P | Energy - Oil & Gas | 12 | 16.5 | 100.0 | 18.22 |
| 5 | Oil - Refiners | Energy - Oil & Gas | 9 | 16.21 | 100.0 | 17.04 |
| 6 | Oil - Large Cap | Energy - Oil & Gas | 8 | 15.16 | 100.0 | 27.49 |
| 7 | Oil - Offshore Drillers | Energy - Oil & Gas | 7 | 14.15 | 80.0 | 31.93 |
| 8 | Chemicals - Plastics | Materials & Chemicals | 5 | 13.99 | 60.0 | 39.26 |
| 9 | Cybersecurity - Network | Technology - Software | 6 | 12.47 | 50.0 | 78.52 |
| 10 | Oil - Permian Basin | Energy - Oil & Gas | 7 | 12.28 | 100.0 | 42.66 |

## Largest Upward Movers Vs Baseline

### Confidence Adjusted
| theme | category | ticker_count | baseline_rank | confidence_rank_delta_vs_baseline | top_abs_share_1m_pct | positive_1m_breadth_pct |
| --- | --- | --- | --- | --- | --- | --- |
| Prisons | Specialty Themes | 2 | 279 | 67 | 62.33 | 0.0 |
| Cruise Lines | Consumer - Leisure | 3 | 252 | 41 | 42.53 | 0.0 |
| Homebuilders - Entry Level | Real Estate - Non REIT | 3 | 241 | 37 | 35.64 | 0.0 |
| Insurance - Title | Financials - Insurance | 4 | 292 | 33 | 37.22 | 0.0 |
| Telecom - Fiber | Telecom | 4 | 303 | 29 | 50.8 | 0.0 |
| Beverages - Spirits | Consumer - Food & Beverage | 4 | 313 | 23 | 39.94 | 0.0 |
| Airlines - International | Industrials - Transport | 3 | 328 | 18 | 37.94 | 0.0 |
| REITs - Timber | Financials - REITs | 4 | 198 | 17 | 41.05 | 25.0 |
| Nuclear - SMR | Energy - Clean | 5 | 297 | 17 | 35.06 | 20.0 |
| Aircraft Leasing | Space & Aerospace | 5 | 265 | 16 | 73.35 | 25.0 |

### Winsorized
| theme | category | ticker_count | baseline_rank | winsorized_rank_delta_vs_baseline | top_abs_share_1m_pct | positive_1m_breadth_pct |
| --- | --- | --- | --- | --- | --- | --- |
| Space - Launch | Space & Aerospace | 7 | 159 | 59 | 64.57 | 0.0 |
| Autonomous Vehicles | Autos & EV | 9 | 309 | 31 | 46.08 | 25.0 |
| Retail - Pet | Consumer - Retail | 5 | 308 | 31 | 67.21 | 20.0 |
| Retail - Grocery | Consumer - Retail | 6 | 245 | 31 | 47.43 | 33.33 |
| Pet Industry | Specialty Themes | 8 | 306 | 27 | 47.74 | 12.5 |
| Auto Parts | Autos & EV | 9 | 249 | 25 | 43.61 | 12.5 |
| Diagnostics & Labs | Healthcare - Devices & Services | 9 | 280 | 22 | 34.07 | 25.0 |
| Media - Streaming | Media & Entertainment | 7 | 171 | 22 | 24.9 | 57.14 |
| LIDAR | Autos & EV | 8 | 314 | 22 | 49.8 | 42.86 |
| EV - Manufacturers | Autos & EV | 11 | 154 | 21 | 24.71 | 37.5 |

### Breadth Adjusted
| theme | category | ticker_count | baseline_rank | breadth_rank_delta_vs_baseline | top_abs_share_1m_pct | positive_1m_breadth_pct |
| --- | --- | --- | --- | --- | --- | --- |
| Cloud - DevOps | Technology - Software | 7 | 258 | 39 | 48.92 | 60.0 |
| Biotech - Cell Therapy | Healthcare - Biotech | 7 | 111 | 37 | 61.87 | 100.0 |
| AI - Enterprise Apps | Artificial Intelligence | 9 | 277 | 35 | 26.68 | 66.67 |
| Software - Enterprise | Technology - Software | 10 | 269 | 33 | 26.46 | 60.0 |
| Telecom - Infrastructure | Telecom | 5 | 79 | 29 | 33.17 | 100.0 |
| Software - Vertical | Technology - Software | 7 | 218 | 28 | 42.01 | 57.14 |
| AI - Software | Artificial Intelligence | 15 | 262 | 28 | 17.29 | 50.0 |
| Water - Utilities | Commodities & Agriculture | 5 | 83 | 27 | 52.97 | 100.0 |
| Biotech - Rare Disease | Healthcare - Biotech | 7 | 146 | 25 | 40.94 | 66.67 |
| Healthcare IT | Healthcare - Devices & Services | 9 | 137 | 25 | 32.52 | 62.5 |

### Combined
| theme | category | ticker_count | baseline_rank | combined_rank_delta_vs_baseline | top_abs_share_1m_pct | positive_1m_breadth_pct |
| --- | --- | --- | --- | --- | --- | --- |
| Prisons | Specialty Themes | 2 | 279 | 66 | 62.33 | 0.0 |
| Retail - Pet | Consumer - Retail | 5 | 308 | 55 | 67.21 | 20.0 |
| Retail - Grocery | Consumer - Retail | 6 | 245 | 45 | 47.43 | 33.33 |
| Biotech - Cell Therapy | Healthcare - Biotech | 7 | 111 | 44 | 61.87 | 100.0 |
| Utilities - Water | Utilities | 7 | 145 | 39 | 38.16 | 66.67 |
| Media - Streaming | Media & Entertainment | 7 | 171 | 38 | 24.9 | 57.14 |
| Cruise Lines | Consumer - Leisure | 3 | 252 | 35 | 42.53 | 0.0 |
| Solar - Utility | Energy - Clean | 5 | 237 | 35 | 60.41 | 20.0 |
| Cloud - DevOps | Technology - Software | 7 | 258 | 34 | 48.92 | 60.0 |
| Homebuilders - Entry Level | Real Estate - Non REIT | 3 | 241 | 33 | 35.64 | 0.0 |

## Small Themes De-emphasized By Combined Variant

| theme | category | ticker_count | baseline_rank | combined_rank | combined_rank_delta_vs_baseline |
| --- | --- | --- | --- | --- | --- |
| Zinc & Lead | Metals & Mining | 3 | 84 | 111 | -27 |
| Retail - Office | Consumer - Retail | 2 | 29 | 56 | -27 |
| Men's Health | Healthcare - Specialty | 3 | 41 | 58 | -17 |
| Industrial Gases | Materials & Chemicals | 3 | 81 | 96 | -15 |
| Shipping - Container | Industrials - Transport | 3 | 13 | 28 | -15 |
| Telecom - Wireless | Telecom | 3 | 51 | 54 | -3 |
| Restaurants - Pizza | Consumer - Restaurants | 3 | 77 | 80 | -3 |
| Test Theme | Tech | 1 | 128 | 130 | -2 |
| Platinum & Palladium | Metals & Mining | 3 | 123 | 123 | 0 |
| Retail - Electronics | Consumer - Retail | 3 | 181 | 173 | 8 |

## Concentrated Themes De-emphasized By Combined Variant

| theme | category | ticker_count | baseline_rank | combined_rank | combined_rank_delta_vs_baseline | top_abs_share_1m_pct |
| --- | --- | --- | --- | --- | --- | --- |
| Meme Stocks | Specialty Themes | 9 | 57 | 143 | -86 | 63.03 |
| Beverages - Soft Drinks | Consumer - Food & Beverage | 6 | 74 | 109 | -35 | 69.41 |
| Retail - Office | Consumer - Retail | 2 | 29 | 56 | -27 | 82.24 |
| Semis - Packaging & Test | Semiconductors | 5 | 20 | 43 | -23 | 64.0 |
| Men's Health | Healthcare - Specialty | 3 | 41 | 58 | -17 | 88.81 |
| Shipping - Container | Industrials - Transport | 3 | 13 | 28 | -15 | 68.07 |
| Behavioral Health | Healthcare - Specialty | 4 | 15 | 30 | -15 | 86.01 |
| Mining Equipment | Industrials - Machinery | 4 | 71 | 79 | -8 | 73.93 |
| Cybersecurity - Network | Technology - Software | 6 | 4 | 9 | -5 | 78.52 |
| Restaurants - Pizza | Consumer - Restaurants | 3 | 77 | 80 | -3 | 86.9 |
