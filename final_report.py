"""
Final Report Generation Module

Generates comprehensive final report consolidating all analysis results.
Includes main report, appendices, and DFSI (Data Fidelity & Security Index) calculation.
"""

import csv
from pathlib import Path

# Import configuration from centralized config module
try:
    from config import (
        OUTPUT_REPORT_FOLDER,
        BLACK_MMSI_LIST,
        BLACK_IMO_LIST
    )
except ImportError:
    # Fallback configuration if config module is not available
    PRIMARY_FOLDER = Path.home() / "Maritime_Shadow_Fleet_Detection"
    OUTPUT_REPORT_FOLDER = PRIMARY_FOLDER / "Data_analysis_and_outputs" / "output3"
    BLACK_MMSI_LIST = []
    BLACK_IMO_LIST = []


def generate_main_report(folder):
    """Generate main report sections (a), (b), (c), and (d)."""
    report = []
    report.append("=" * 60)
    report.append("              FINAL COMPREHENSIVE REPORT")
    report.append("=" * 60 + "\n")

    # --- (a and b) Duplicate IMO/MMSI ---
    report.append("--- SECTION 1: VESSEL IDENTIFICATION ANOMALIES ---")
    ab_files = [
        ('IMO_with_multiple_MMSI.csv', '1.1 IMO Numbers with Multiple MMSI', 'IMO', 'MMSI_List', 'sailing with MMSI numbers'),
        ('MMSI_with_multiple_IMO.csv', '1.2 MMSI Numbers with Multiple IMO', 'MMSI', 'IMO_List', 'associated with multiple IMO')
    ]

    for filename, title, key1, key2, text in ab_files:
        report.append(f"\n{title}:")
        filepath = folder / filename
        if filepath.exists():
            has_data = False
            with open(filepath, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    has_data = True
                    report.append(f"  • {key1}: {row[key1]} {text}: {row[key2]}")
            if not has_data:
                report.append("  No data found.")
        else:
            report.append(f"  ⚠ File {filename} not found.")
    
    report.append("")

    # --- (c) Master file line counts ---
    report.append("--- SECTION 2: MASTER DATA OVERVIEW ---")
    for fname in ['master_IMO_data.csv', 'master_MMSI_data.csv']:
        filepath = folder / fname
        if filepath.exists():
            with open(filepath, mode='r', encoding='utf-8') as f:
                count = sum(1 for _ in f) - 1
                report.append(f"  • {fname}: {max(0, count):,} records")
        else:
            report.append(f"  ⚠ File {fname} not found.")
    report.append("")

    # --- (d) Mobile Type Summary ---
    report.append("--- SECTION 3: MOBILE TYPE SUMMARY ---")
    summary_f = folder / 'master_list___Mobile_by_Type_summary.csv'
    if summary_f.exists():
        with open(summary_f, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            report.append(f"{'Mobile Type':<35} | {'Count':>10} | {'Unique MMSI':>12} | {'Unique IMO':>10}")
            report.append("-" * 80)
            for row in reader:
                report.append(f"{row['Mobile Type']:<35} | {int(row['Count']):>10,} | {int(row['Unique MMSI']):>12,} | {int(row['Unique IMO']):>10,}")
    else:
        report.append("  ⚠ Summary file not found.")
    report.append("")

    return "\n".join(report)


def generate_appendix_a(folder):
    """Appendix A: Gap Analysis with filters (>4h, >10km, MMSI_changed)."""
    # Find the gap analysis file (use most recent if multiple exist)
    gap_files = list(folder.glob('gap_analysis_report_*.csv'))
    if not gap_files:
        gap_files = list(folder.glob('gap_analysis_report_.csv'))
    
    appendix = ["\n--- APPENDIX A: DETAILED GAP ANALYSIS (GAP > 4h & DIST > 10km) ---"]

    if not gap_files:
        appendix.append("⚠ Gap analysis report file not found.")
        return "\n".join(appendix)
    
    # Use the first gap file found
    gap_file = gap_files[0]
    appendix.append(f"Source: {gap_file.name}\n")

    mmsi_over_4h = set()
    dist_over_10km = []
    mmsi_changes = []

    with open(gap_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            mmsi = row['MMSI']
            try:
                gap_h = float(row['Gap_Hours'])
                dist_km = float(row['Distance_km'])
                changed = int(row['MMSI_changed'])
            except (ValueError, KeyError):
                continue

            # Filter 1: Gaps longer than 4 hours
            if gap_h > 4.0:
                mmsi_over_4h.add(mmsi)
            
            # Filter 2: Distance over 10km
            if dist_km > 10.0:
                dist_over_10km.append(
                    f"  • MMSI: {mmsi:<12} | Distance: {dist_km:>8.2f} km | Gap: {gap_h:>6.2f} h | Time: {row['Start_Time']}"
                )
            
            # Filter 3: MMSI change
            if changed != 0:
                mmsi_changes.append(
                    f"  ⚠ CRITICAL: Vessel {mmsi} changed MMSI (Time: {row['Start_Time']})"
                )

    appendix.append(f"1. Vessels (MMSI) with gaps > 4 hours (Total: {len(mmsi_over_4h)}):")
    if mmsi_over_4h:
        for mmsi in sorted(mmsi_over_4h):
            appendix.append(f"    - {mmsi}")
    else:
        appendix.append("    None found")
    
    appendix.append(f"\n2. Cases where distance traveled during gap exceeded 10 km (Total: {len(dist_over_10km)}):")
    appendix.extend(dist_over_10km if dist_over_10km else ["    None found"])

    if mmsi_changes:
        appendix.append("\n3. CRITICAL MMSI CHANGES DETECTED:")
        appendix.extend(mmsi_changes)

    return "\n".join(appendix)


def generate_appendix_b(folder):
    """Appendix B: Vessel proximity meetings."""
    meeting_f = folder / 'vessel_proximity_meetings.csv'
    appendix = ["\n--- APPENDIX B: VESSEL PROXIMITY MEETINGS ---"]

    if meeting_f.exists():
        has_data = False
        with open(meeting_f, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                has_data = True
                appendix.append(
                    f"  • Vessels {row['MMSI_1']} and {row['MMSI_2']} sailed close together "
                    f"from {row['Start_Time']} to {row['End_Time']} "
                    f"at avg speed {row['Avg_SOG_Combined']} knots"
                )
        if not has_data:
            appendix.append("  No proximity meetings detected.")
    else:
        appendix.append("  ⚠ File vessel_proximity_meetings.csv not found.")
    
    return "\n".join(appendix)


def generate_appendix_c(folder):
    """Appendix C: Draught change analysis (excluding 100% errors)."""
    dr_f = folder / 'mmsi_draught_change.csv'
    appendix = ["\n--- APPENDIX C: DRAUGHT CHANGE ANALYSIS (Excluding 100% errors) ---"]

    if dr_f.exists():
        found_cases = []
        with open(dr_f, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    pct = float(row['Change_Percent'].replace('%', ''))
                except ValueError:
                    continue
                
                if pct != 100.0:
                    found_cases.append(
                        f"  • MMSI: {row['MMSI']:<12} | "
                        f"Draught: {row['Draught_Before']}m → {row['Draught_After']}m | "
                        f"Change: {row['Change_Percent']}"
                    )
        
        if found_cases:
            appendix.extend(found_cases)
        else:
            appendix.append("  No real draught changes (non-100%) found.")
    else:
        appendix.append("  ⚠ File mmsi_draught_change.csv not found.")
    
    return "\n".join(appendix)


def generate_appendix_d(folder):
    """Appendix D: Outlier summary."""
    out_f = folder / 'mmsi_outlier_summary.csv'
    appendix = ["\n--- APPENDIX D: DATA OUTLIER SUMMARY ---"]

    if out_f.exists():
        with open(out_f, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            appendix.append(f"{'MMSI':<12} | {'Total Points':>12} | {'Removed':>10} | {'Filtered Distance (KM)':>22}")
            appendix.append("-" * 75)
            for row in reader:
                dist = f"{float(row['Total_Filtered_Dist_KM']):,.2f}"
                appendix.append(
                    f"{row['MMSI']:<12} | {int(row['Total_Points']):>12,} | "
                    f"{int(row['Removed_Points']):>10,} | {dist:>22}"
                )
    else:
        appendix.append("  ⚠ File mmsi_outlier_summary.csv not found.")
    
    return "\n".join(appendix)


def generate_dfsi_analysis(folder):
    """
    Calculate DFSI (Data Fidelity & Security Index) for all vessels.
    Saves full list and sorted high-risk list (DFSI > 0).
    
    DFSI Formula: (Max_Gap / 2) + (Total_Jump / 10) + (Draught_Changes * 15)
    """
    dfsi_data = {}

    # 1. Initial list from master MMSI
    master_f = folder / 'master_MMSI_data.csv'
    if master_f.exists():
        with open(master_f, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                dfsi_data[row['MMSI']] = {'gap': 0.0, 'jump': 0.0, 'draught': 0}

    # 2. Data collection (Gap, Draught, Jump)
    # --- Max Gap ---
    gap_files = list(folder.glob('gap_analysis_report_*.csv'))
    if gap_files:
        with open(gap_files[0], mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                m = row['MMSI']
                if m in dfsi_data:
                    val = float(row.get('Gap_Hours', 0))
                    if val > dfsi_data[m]['gap']:
                        dfsi_data[m]['gap'] = val

    # --- Draught Changes ---
    dr_f = folder / 'mmsi_draught_change.csv'
    if dr_f.exists():
        with open(dr_f, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                m = row['MMSI']
                if m in dfsi_data:
                    try:
                        if float(row['Change_Percent'].replace('%', '')) != 100.0:
                            dfsi_data[m]['draught'] += 1
                    except:
                        continue

    # --- Impossible Jumps ---
    out_f = folder / 'mmsi_outlier_summary.csv'
    if out_f.exists():
        with open(out_f, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                m = row['MMSI']
                if m in dfsi_data:
                    dfsi_data[m]['jump'] = float(row.get('Total_Filtered_Dist_KM', 0))

    # 3. Calculation and list preparation
    all_results = []
    
    for mmsi, d in dfsi_data.items():
        index = (d['gap'] / 2) + (d['jump'] / 10) + (d['draught'] * 15)
        all_results.append({
            'MMSI': mmsi,
            'DFSI': round(index, 2),
            'Max_Gap': d['gap'],
            'Draught_Changes': d['draught'],
            'Total_Jump': d['jump']
        })

    # 4. FILTERING AND SORTING
    # Only those with DFSI > 0
    positive_only = [r for r in all_results if r['DFSI'] > 0]
    # Sort by DFSI (descending: reverse=True)
    sorted_positive = sorted(positive_only, key=lambda x: x['DFSI'], reverse=True)

    # 5. SAVE TO FILES
    fieldnames = ['MMSI', 'DFSI', 'Max_Gap', 'Draught_Changes', 'Total_Jump']
    
    # File A: All results
    with open(folder / "dfsi_full_list.csv", 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_results)

    # File B: Only > 0 and sorted (High-risk vessels)
    with open(folder / "dfsi_high_risk_sorted.csv", 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted_positive)

    # 6. Report text generation (using sorted list)
    report = ["\n--- SECTION 4: DFSI - DATA FIDELITY & SECURITY INDEX (Sorted by Risk) ---"]
    report.append(f"{'MMSI':<12} | {'Max Gap':>8} | {'Dr.Chg':>7} | {'Jump KM':>10} | {'DFSI INDEX':>12}")
    report.append("-" * 65)

    if not sorted_positive:
        report.append("No suspicious activity detected (DFSI > 0).")
    else:
        for r in sorted_positive:
            report.append(
                f"{r['MMSI']:<12} | {r['Max_Gap']:>8.1f} | {r['Draught_Changes']:>7} | "
                f"{r['Total_Jump']:>10.1f} | {r['DFSI']:>12.2f}"
            )

    return "\n".join(report)


def add_config_info():
    """Add information from config.py (blacklists)."""
    mmsi_b = BLACK_MMSI_LIST if BLACK_MMSI_LIST else []
    imo_b = BLACK_IMO_LIST if BLACK_IMO_LIST else []
    info = ["\n--- SECTION 5: CONFIGURATION DATA (Blacklists) ---"]
    info.append(f"  • BLACK_MMSI_LIST: {mmsi_b if mmsi_b else 'Empty'}")
    info.append(f"  • BLACK_IMO_LIST: {imo_b if imo_b else 'Empty'}")
    return "\n".join(info)


def run_final_report_generation():
    """
    Main function to generate comprehensive final report.
    
    This function:
    1. Gathers results from all previous analyses
    2. Generates main report and appendices
    3. Calculates DFSI risk index
    4. Saves comprehensive report to file
    5. Displays report to console
    
    Returns:
        str: Path to generated report file
    """
    folder = OUTPUT_REPORT_FOLDER
    
    # Ensure folder exists
    folder.mkdir(parents=True, exist_ok=True)
    
    print("\n" + "=" * 60)
    print("FINAL REPORT GENERATION")
    print("=" * 60)
    print(f"\nGenerating comprehensive report from:")
    print(f"  {folder}")
    print("\nThis will consolidate results from all analyses:")
    print("  ✓ Vessel identification anomalies")
    print("  ✓ Master data overview")
    print("  ✓ Mobile type summary")
    print("  ✓ Gap analysis (Appendix A)")
    print("  ✓ Proximity meetings (Appendix B)")
    print("  ✓ Draught changes (Appendix C)")
    print("  ✓ Outlier summary (Appendix D)")
    print("  ✓ DFSI risk index calculation")
    print("=" * 60 + "\n")
    
    try:
        # Generate all report sections
        report_sections = [
            generate_main_report(folder),
            generate_appendix_a(folder),
            generate_appendix_b(folder),
            generate_appendix_c(folder),
            generate_appendix_d(folder),
            generate_dfsi_analysis(folder),
            add_config_info(),
            "\n" + "=" * 60,
            "                END OF REPORT",
            "=" * 60
        ]
        
        output_text = "\n".join(report_sections)
        
        # Save report to file
        output_file = folder / "Final_Comprehensive_Report.txt"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(output_text)
        
        # Display report
        print(output_text)
        
        print("\n" + "=" * 60)
        print("✅ REPORT GENERATION COMPLETE")
        print("=" * 60)
        print(f"\n📄 Report saved to: {output_file}")
        print(f"📊 DFSI full list: {folder / 'dfsi_full_list.csv'}")
        print(f"🔴 High-risk vessels (DFSI > 0): {folder / 'dfsi_high_risk_sorted.csv'}")
        print("=" * 60)
        
        return str(output_file)
        
    except Exception as e:
        print(f"\n❌ Error generating final report: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    try:
        run_final_report_generation()
    except KeyboardInterrupt:
        print("\n\nStopped by user.")
