# -*- coding: utf-8 -*-
"""Search statistics tracker for POI matching stages."""

import logging
from collections import defaultdict


class SearchStatistics:
    """Track which stage each POI was found in (stages 1-4)."""

    def __init__(self):
        self.stage_results = {1: 0, 2: 0, 3: 0, 4: 0}
        self.found_in_stage = defaultdict(list)  # {stage: [poi_ids]}
        self.not_found = []
        self.total_processed = 0

    def record_found(self, poi_id, stage):
        """Record: POI found in given stage."""
        if stage not in [1, 2, 3, 4]:
            logging.warning(f"Invalid stage {stage} for POI {poi_id}")
            return

        self.stage_results[stage] += 1
        self.found_in_stage[stage].append(poi_id)
        self.total_processed += 1

    def record_not_found(self, poi_id):
        """Record: POI not found in any stage."""
        self.not_found.append(poi_id)
        self.total_processed += 1

    def get_report(self):
        """Get formatted statistics report."""
        if self.total_processed == 0:
            return {
                'stage_1': '0 (0.0%)',
                'stage_2': '0 (0.0%)',
                'stage_3': '0 (0.0%)',
                'stage_4': '0 (0.0%)',
                'not_found': '0 (0.0%)',
                'total': 0,
                'success_rate': '0.0%',
            }

        not_found_count = len(self.not_found)
        success_rate = (self.total_processed - not_found_count) / self.total_processed * 100

        return {
            'stage_1': f"{self.stage_results[1]} ({self.stage_results[1]/self.total_processed*100:.1f}%)",
            'stage_2': f"{self.stage_results[2]} ({self.stage_results[2]/self.total_processed*100:.1f}%)",
            'stage_3': f"{self.stage_results[3]} ({self.stage_results[3]/self.total_processed*100:.1f}%)",
            'stage_4': f"{self.stage_results[4]} ({self.stage_results[4]/self.total_processed*100:.1f}%)",
            'not_found': f"{not_found_count} ({not_found_count/self.total_processed*100:.1f}%)",
            'total': self.total_processed,
            'success_rate': f"{success_rate:.1f}%",
        }

    def log_summary(self):
        """Log formatted statistics summary."""
        report = self.get_report()

        lines = [
            "=" * 50,
            "🔍 Search Statistics (4 Stages)",
            "=" * 50,
            f"Stage 1 (Name search):      {report['stage_1']}",
            f"Stage 2 (Address search):   {report['stage_2']}",
            f"Stage 3 (Distance search):  {report['stage_3']}",
            f"Stage 4 (Fuzzy search):     {report['stage_4']}",
            "-" * 50,
            f"Not found:                  {report['not_found']}",
            f"TOTAL:                      {report['total']} items",
            "=" * 50,
            f"Success Rate: {report['success_rate']} ✅" if float(report['success_rate'].rstrip('%')) > 90 else f"Success Rate: {report['success_rate']} ⚠️",
            "=" * 50,
        ]

        logging.info('\n'.join(lines))

    def export_json(self):
        """Export statistics as JSON-serializable dict."""
        return {
            'by_stage': dict(self.stage_results),
            'not_found_count': len(self.not_found),
            'total_processed': self.total_processed,
            'success_rate_percent': (self.total_processed - len(self.not_found)) / self.total_processed * 100 if self.total_processed > 0 else 0,
            'found_in_stage': {k: len(v) for k, v in self.found_in_stage.items()},
        }


# Global instance for singleton pattern
_global_stats = None


def get_search_statistics():
    """Get or create global statistics instance."""
    global _global_stats
    if _global_stats is None:
        _global_stats = SearchStatistics()
    return _global_stats


def reset_search_statistics():
    """Reset global statistics instance."""
    global _global_stats
    _global_stats = None
