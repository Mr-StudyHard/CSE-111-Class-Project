// /movie-tv-analytics/web/src/api.ts
import axios from 'axios'

const baseURL = (import.meta as any).env?.VITE_API_URL || '/api'
export const api = axios.create({ baseURL })

export type MediaItem = {
	id?: number
	tmdb_id: number
	media_type: 'movie' | 'tv'
	title: string
	overview?: string
	poster_path?: string
	backdrop_path?: string
	vote_average?: number
	vote_count?: number
	popularity?: number
	release_date?: string
	genres?: string[]
	original_language?: string
}

export async function getSummary() {
	const { data } = await api.get('/summary')
	return data as {
		total_items: number
		movies: number
		tv: number
		avg_rating: number
		top_genres: { genre: string; count: number }[]
		languages: { language: string; count: number }[]
	}
}

export async function getList(type: 'movie' | 'tv', sort = 'popularity', page = 1, limit = 20) {
	const path = type === 'movie' ? '/movies' : '/tv'
	const { data } = await api.get(path, { params: { sort, page, limit } })
	return data as { total: number; page: number; results: MediaItem[] }
}

export async function refresh(pages = 1) {
	const { data } = await api.post('/refresh', null, { params: { pages } })
	return data as { ok: boolean; inserted: number; updated: number; total: number }
}

export async function search(q: string, page = 1) {
	const { data } = await api.get('/search', { params: { q, page } })
	return data as { page: number; results: MediaItem[]; total_results: number }
}
