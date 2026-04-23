export type ApiStatus = "checking" | "connected" | "error";
export interface HealthResponse { ok: boolean }

export type Page = "list" | "event-record" | "night-of-actuals" | "post-event-review" | "historical";

export interface EventSummary {
  id: number
  event_name: string
  event_date: string
  day_of_week: string
  tier1_category: string
  promoter_name: string
  projected_total_revenue: number
  review_status: string
}

export interface EventFull extends EventSummary {
  tier2_subcategory: string
  artist_name: string
  artist_genre: string
  expected_attendance: number
  venue_capacity: number
  deal_structure_type: string
  door_split_venue: number
  door_split_promoter: number
  bar_split_venue: number
  bar_split_promoter: number
  bar_split_basis: string
  bar_cogs_percentage: number
  bar_threshold_amount: number
  bar_guarantee_amount: number
  door_split_basis: string
  door_threshold_amount: number
  door_guarantee_amount: number
  table_minimum: number
  table_split_promoter_overage: number
  deposit_collection_method: string
  artist_fee_landed: number
  artist_fee_travel: number
  artist_cost_responsibility: string
  artist_cost_split_note: string
  deal_notes: string
  // Net revenue split
  net_revenue_promoter_pct: number
  net_revenue_venue_pct: number
  net_revenue_basis: string
  // Rebuilt table fields
  table_split_promoter: number
  table_split_basis: string
  // Structured artist cost split
  artist_cost_split_method: string
  artist_venue_dollar: number
  artist_venue_pct: number
  artist_promoter_dollar: number
  deposit_amount: number
  deposit_due_date: string
  balance_due: number
  balance_due_date: string
  projected_door_revenue: number
  projected_bar_revenue: number
  projected_table_revenue: number
  notes: string
}

export interface NightOfActuals {
  id?: number
  event_id: number
  time_of_entry: string
  total_bar_sales: number
  liquor_sales: number
  beer_wine_sales: number
  table_bottle_service: number
  comps_total: number
  voids: number
  tax_collected: number
  tips: number
  door_revenue_cash: number
  door_revenue_card: number
  total_headcount: number
  incident_description: string
  incident_department: string
  notes: string
  bar_cogs_deduction: number
  bar_threshold_retained: number
  door_threshold_retained: number
  charge_backs: number
  promoter_bar_payout: number
  promoter_door_payout: number
  promoter_table_payout: number
  artist_cost_paid_by_venue: number
  effective_split_percentage: number | null
  settlement_notes: string
}

export interface PostEventReview {
  id?: number
  event_id: number
  actual_attendance: number
  actual_door_revenue: number
  actual_bar_revenue: number
  actual_table_revenue: number
  artist_cost_actual: number
  staffing_cost_actual: number
  crowd_demographic_observations: string
  customer_service_observations: string
  operational_breakdowns: string
  what_to_replicate: string
  what_to_change: string
  promoter_performance_notes: string
  inventory_observations: string
  staffing_observations: string
  promoter_artwork_on_time: string
  promoter_social_posting: string
  promoter_attendance_vs_projection: string
  promoter_role_boundaries: string
  review_status: string
  projected_effective_split: number | null
  actual_effective_split: number | null
  settlement_notes_reference: string
}

export interface HistoricalEvent {
  id?: number
  event_date: string
  event_name: string
  tier1_category: string
  tier2_subcategory: string
  promoter_name: string
  artist_name: string
  gross_revenue: number
  attendance: number
  data_source: string
  classification_status: string
}

export const TIER1_CATEGORIES = [
  "Electronic", "Open Format", "Hip Hop and R&B", "Latin",
  "Live Performance", "Corporate and Private", "Sports and Viewing", "Themed and Holiday",
] as const

export const DEAL_STRUCTURES = [
  "Door split", "Bar split", "Flat guarantee", "Versus deal", "Hybrid", "Net revenue split",
] as const

export const BAR_SPLIT_BASIS_OPTIONS = [
  "Gross bar sales",
  "Net after COGS",
  "Net after first threshold",
  "Net after guarantee",
  "Custom",
] as const

export const DOOR_SPLIT_BASIS_OPTIONS = [
  "Gross door revenue",
  "Net after credit card fees",
  "Net after first threshold to venue",
  "After venue guarantee",
  "Flat guarantee to promoter",
  "Custom",
] as const

export const DEPOSIT_COLLECTION_OPTIONS = [
  "Venue collects all deposits",
  "Promoter collects with required disclosure",
  "Split collection",
] as const

export const NET_REVENUE_BASIS_OPTIONS = [
  "Net after tax tips and comps",
  "Net after tax tips comps and COGS",
  "Net after all deductions including house fee",
  "Custom",
] as const

export const TABLE_SPLIT_BASIS_OPTIONS = [
  "Net after tax and tips",
  "Net after tax tips and credit card fees",
  "Custom",
] as const

export const ARTIST_COST_SPLIT_METHOD_OPTIONS = [
  "By dollar amount",
  "By percentage",
] as const

export const ARTIST_COST_RESPONSIBILITY_OPTIONS = [
  "Venue pays full cost",
  "Promoter pays full cost",
  "Split per deal terms",
] as const

export const TIME_OF_ENTRY_OPTIONS = [
  "10 PM check-in", "11 PM check-in", "12 AM check-in", "1 AM check-in", "Close",
] as const

export const HIT_MISSED_OPTIONS = ["Hit", "Missed", "Partial", "N/A"] as const

export const DATA_SOURCE_OPTIONS = [
  "Revel export", "R365 export", "Closeout report", "Memory",
] as const

export const CLASSIFICATION_STATUS_OPTIONS = [
  "Complete", "Partial", "Needs review",
] as const
