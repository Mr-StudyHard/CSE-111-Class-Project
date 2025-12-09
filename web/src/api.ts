// /movie-tv-analytics/web/src/api.ts
import axios from 'axios'

// Always talk to the same origin as the browser.
// In dev, Vite proxies "/api" to the Flask server (see vite.config.ts).
// You can still override with VITE_API_URL if you need an absolute URL.
const envUrl = (import.meta as any).env?.VITE_API_URL as string | undefined
const baseURL = envUrl && envUrl.trim().length > 0 ? envUrl : '/api'
export const api = axios.create({ baseURL })

// Auth token management for admin endpoints
let authToken: string | null = null

export function setAuthToken(userId: number | undefined, email: string | undefined) {
	if (userId && email) {
		authToken = `${userId}:${email}`
	} else {
		authToken = null
	}
}

export function clearAuthToken() {
	authToken = null
}

// Add auth header to requests that need it
api.interceptors.request.use((config) => {
	if (authToken) {
		config.headers.Authorization = `Bearer ${authToken}`
	}
	return config
})

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

export type TrendingItem = {
	item_id: number
	tmdb_id: number
	media_type: 'movie' | 'show'
	title: string
	overview?: string
	poster_url?: string | null
	backdrop_url?: string | null
	tmdb_vote_avg?: number | null
	release_date?: string | null
	genres: string[]
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

export async function getList(
	type: 'movie' | 'tv', 
	sort = 'popularity', 
	page = 1, 
	limit = 20, 
	genre?: string, 
	language?: string
) {
	const path = type === 'movie' ? '/movies' : '/tv'
	const params: any = { sort, page, limit }
	if (genre && genre !== 'all') params.genre = genre
	if (language && language !== 'all') params.language = language
	const { data } = await api.get(path, { params })
	return data as { total: number; page: number; results: MediaItem[] }
}

export async function getGenres() {
	const { data } = await api.get('/genres')
	return data as { genres: string[] }
}

export async function getLanguages() {
	const { data } = await api.get('/languages')
	return data as { languages: string[] }
}

export async function refresh(pages = 1) {
	const { data } = await api.post('/refresh', null, { params: { pages } })
	return data as { ok: boolean; inserted: number; updated: number; total: number }
}

export async function search(q: string, page = 1) {
	const { data } = await api.get('/search', { params: { q, page } })
	return data as { page: number; results: MediaItem[]; total_results: number }
}

export type UserRow = { user: string; email: string; password: string }

export async function getUsers() {
	const { data } = await api.get('/users')
	return data as UserRow[]
}

export async function getUserByEmail(email: string) {
	try {
		const { data } = await api.get('/user/by-email', { params: { email } })
		// Backend returns {ok: true, user_id, email} on success or {ok: false, error} on failure
		if(data.ok) {
			return data as { ok: boolean; user_id?: number; email?: string; error?: string }
		} else {
			return { ok: false, error: data.error || 'User not found' }
		}
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to get user'
		return { ok: false, error: String(msg) }
	}
}

export async function getHealth() {
	const { data } = await api.get('/health')
	return data as { status: string }
}

export async function getTrending(period: 'weekly' | 'monthly' | 'all' = 'weekly', limit = 20) {
	const { data } = await api.get('/trending', { params: { period, limit } })
	return (data?.results ?? []) as TrendingItem[]
}

export async function getNewReleases(limit = 12, type: 'all' | 'movie' | 'tv' = 'all') {
	const { data } = await api.get('/new-releases', { params: { limit, type } })
	return (data?.results ?? []) as MediaItem[]
}

export type LoginResponse = { ok: true; user: string; email: string; user_id?: number; display_name?: string; is_admin?: boolean } | { ok: false; error: string }

export async function login(email: string, password: string) {
	try {
		const { data } = await api.post('/login', { email, password })
		return data as LoginResponse
	} catch (err: any) {
		// Normalize axios errors into a consistent shape so UI can handle them
		const msg = (err?.response?.data?.error) || err?.message || 'Login request failed'
		return { ok: false, error: String(msg) } as LoginResponse
	}
}

export type SignupResponse = { ok: true; user: string; email: string } | { ok: false; error: string }

export async function signup(email: string, password: string, username?: string) {
	try {
		const { data } = await api.post('/signup', { email, password, username })
		return data as SignupResponse
	} catch (err: any) {
		const msg = (err?.response?.data?.error) || err?.message || 'Signup request failed'
		return { ok: false, error: String(msg) } as SignupResponse
	}
}

export type CastMember = {
	person_id: number
	name: string
	profile_path?: string | null
	profile_url?: string | null
	character?: string | null
	cast_order?: number | null
}

export type MovieDetail = {
	movie_id: number
	tmdb_id: number
	title: string
	overview: string
	poster_path?: string
	backdrop_path?: string
	release_year?: number
	runtime_minutes?: number
	vote_average?: number
	vote_count?: number
	popularity?: number
	original_language?: string
	budget?: number
	revenue?: number
	genres: string[]
	user_avg_rating?: number
	review_count: number
	consolidated_rating?: number
	media_type: 'movie'
	top_cast?: CastMember[]
}

export type ShowDetail = {
	show_id: number
	tmdb_id: number
	title: string
	overview: string
	poster_path?: string
	backdrop_path?: string
	first_air_date?: string
	last_air_date?: string
	vote_average?: number
	vote_count?: number
	popularity?: number
	original_language?: string
	season_count: number
	genres: string[]
	user_avg_rating?: number
	review_count: number
	consolidated_rating?: number
	media_type: 'tv'
	top_cast?: CastMember[]
}

export async function getMovieDetail(movieId: number) {
	const { data } = await api.get(`/movie/${movieId}`)
	return data as MovieDetail
}

export async function getShowDetail(showId: number) {
	const { data } = await api.get(`/show/${showId}`)
	return data as ShowDetail
}

export async function uploadImage(file: File) {
	const formData = new FormData()
	formData.append('file', file)
	const { data } = await api.post('/upload-image', formData, {
		headers: {
			'Content-Type': 'multipart/form-data',
		},
	})
	return data as { ok: boolean; path?: string; error?: string }
}

export async function deleteMedia(mediaType: 'movie' | 'tv', id: number) {
	// Validate ID
	if (id === undefined || id === null || isNaN(Number(id))) {
		return { ok: false, error: `Invalid ID: ${id}` }
	}
	
	try {
		const path = mediaType === 'movie' ? `/movies/${id}` : `/tv/${id}`
		const fullUrl = `${api.defaults.baseURL}${path}`
		console.log(`[deleteMedia] Calling DELETE ${fullUrl} for ${mediaType} with id ${id}`)
		const { data } = await api.delete(path)
		return data as { ok: boolean; deleted?: number; error?: string }
	} catch (err: any) {
		console.error(`[deleteMedia] Error deleting ${mediaType} ${id}:`, err)
		const status = err?.response?.status
		const statusText = err?.response?.statusText
		const errorData = err?.response?.data
		
		let msg = 'Delete request failed'
		if (status === 404) {
			msg = errorData?.error || `Item with ID ${id} not found (404)`
		} else if (errorData?.error) {
			msg = errorData.error
		} else if (err?.message) {
			msg = err.message
		}
		
		return { ok: false, error: String(msg) }
	}
}

export type Review = {
	review_id: number
	user_id: number
	user_email?: string
	content: string
	rating?: number | null
	created_at: string
	reactions?: Record<string, number> // emote_type -> count
}

export type ReviewResponse = {
	ok: boolean
	reviews: Review[]
	count: number
	error?: string
}

export async function getReviews(targetType: 'movie' | 'show', targetId: number) {
	try {
		const { data } = await api.get('/reviews', {
			params: { target_type: targetType, target_id: targetId }
		})
		return data as ReviewResponse
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to fetch reviews'
		return { ok: false, reviews: [], count: 0, error: String(msg) }
	}
}

export async function createReview(userId: number, targetType: 'movie' | 'show', targetId: number, content: string, rating?: number) {
	try {
		const { data } = await api.post('/reviews', {
			user_id: userId,
			target_type: targetType,
			target_id: targetId,
			content: content,
			rating: rating
		})
		return data as { ok: boolean; review_id?: number; error?: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to create review'
		return { ok: false, error: String(msg) }
	}
}

export async function updateReview(reviewId: number, rating?: number, content?: string) {
	try {
		const { data } = await api.put(`/reviews/${reviewId}`, { rating, content })
		return data as { ok: boolean; error?: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to update review'
		return { ok: false, error: String(msg) }
	}
}

export async function deleteReview(reviewId: number) {
	try {
		const { data } = await api.delete(`/reviews/${reviewId}`)
		return data as { ok: boolean; deleted?: number; error?: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to delete review'
		return { ok: false, error: String(msg) }
	}
}

export type ReactionResponse = {
	ok: boolean
	reactions?: Record<string, number>
	user_reactions?: string[]
	action?: 'added' | 'removed'
	error?: string
}

export async function addReviewReaction(reviewId: number, emoteType: string) {
	try {
		const { data } = await api.post(`/reviews/${reviewId}/reactions`, {
			emote_type: emoteType
		})
		return data as ReactionResponse
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to add reaction'
		return { ok: false, error: String(msg) }
	}
}

export async function getReviewReactions(reviewId: number) {
	try {
		const { data } = await api.get(`/reviews/${reviewId}/reactions`)
		return data as ReactionResponse
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to get reactions'
		return { ok: false, error: String(msg) }
	}
}

export async function updateMedia(mediaType: 'movie' | 'tv', id: number, payload: UpdateMediaPayload) {
	try {
		const path = mediaType === 'movie' ? `/movies/${id}` : `/tv/${id}`
		const { data } = await api.put(path, payload)
		return data as { ok: boolean; id?: number; error?: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Update request failed'
		return { ok: false, error: String(msg) }
	}
}

export async function copyMedia(mediaType: 'movie' | 'tv', id: number) {
	// Validate ID
	if (id === undefined || id === null || isNaN(Number(id))) {
		return { ok: false, error: `Invalid ID: ${id}` }
	}
	
	try {
		// Fetch the original item details
		const detail = mediaType === 'movie' 
			? await getMovieDetail(id)
			: await getShowDetail(id)
		
		// Extract the first genre (required field)
		const genres = detail.genres || []
		const genre = genres.length > 0 ? genres[0] : 'Drama' // Default genre if none found
		
		// Prepare payload for creating a copy
		const payload: CreateMediaPayload = {
			media_type: mediaType,
			title: detail.title,
			overview: detail.overview || undefined,
			language: detail.original_language || undefined,
			poster_path: detail.poster_path || undefined,
			tmdb_score: detail.vote_average || undefined,
			popularity: detail.popularity || undefined,
			genre: genre,
		}
		
		// Add year field based on media type
		if (mediaType === 'movie') {
			const movieDetail = detail as MovieDetail
			if (movieDetail.release_year) {
				payload.release_year = movieDetail.release_year
			}
		} else {
			const showDetail = detail as ShowDetail
			if (showDetail.first_air_date) {
				// Extract year from date string (YYYY-MM-DD or YYYY)
				const yearMatch = showDetail.first_air_date.match(/^(\d{4})/)
				if (yearMatch) {
					payload.first_air_year = parseInt(yearMatch[1])
				}
			}
		}
		
		// Create the copy
		const result = await createMedia(payload)
		return result
	} catch (err: any) {
		console.error(`[copyMedia] Error copying ${mediaType} ${id}:`, err)
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Copy request failed'
		return { ok: false, error: String(msg) }
	}
}

export type CreateMediaPayload = {
	media_type: 'movie' | 'tv'
	title: string
	overview?: string
	language?: string
	release_year?: number
	first_air_year?: number
	tmdb_score?: number
	popularity?: number
	poster_path?: string
	genre: string
}

export type UpdateMediaPayload = {
	title?: string
	overview?: string
	language?: string
	release_year?: number
	first_air_year?: number
	tmdb_score?: number
	popularity?: number
	poster_path?: string
	genre?: string
}

export async function createMedia(payload: CreateMediaPayload) {
	const path = payload.media_type === 'movie' ? '/movies' : '/tv'
	const { media_type, ...body } = payload
	const { data } = await api.post(path, body)
	return data as { ok: boolean; id?: number; tmdb_id?: number; title?: string; error?: string }
}

export type UserSettings = {
	user_id: number
	email: string
	display_name?: string
	created_at?: string
	is_admin: boolean
}

export async function getUserSettings() {
	try {
		const { data } = await api.get('/user/settings')
		return data as { ok: true } & UserSettings | { ok: false; error: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to fetch settings'
		return { ok: false, error: String(msg) } as { ok: false; error: string }
	}
}

export type UpdateSettingsPayload = {
	current_password: string
	display_name?: string
	new_email?: string
	new_password?: string
}

export async function updateUserSettings(payload: UpdateSettingsPayload) {
	try {
		const { data } = await api.put('/user/settings', payload)
		return data as { ok: boolean; message?: string; error?: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to update settings'
		return { ok: false, error: String(msg) }
	}
}

export async function deleteUserAccount(password: string) {
	try {
		const { data } = await api.delete('/user/account', { data: { password } })
		return data as { ok: boolean; message?: string; error?: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to delete account'
		return { ok: false, error: String(msg) }
	}
}

export type UserProfile = {
	user: {
		user_id: number
		email: string
		display_name?: string
		created_at?: string
	}
	stats: {
		movies: {
			review_count: number
			avg_rating: number
			estimated_hours: number
			discussion_count: number
		}
		tv: {
			review_count: number
			avg_rating: number
			estimated_hours: number
			discussion_count: number
		}
	}
	favorites: {
		movies: Array<{
			title: string
			media_type: 'movie'
			rating?: number
			id?: number
			poster_path?: string | null
		}>
		tv: Array<{
			title: string
			media_type: 'tv'
			rating?: number
			id?: number
			poster_path?: string | null
		}>
	}
	watchlist: {
		movies: Array<{
			title: string
			media_type: 'movie'
			id?: number
			added_at?: string
			poster_path?: string | null
		}>
		tv: Array<{
			title: string
			media_type: 'tv'
			id?: number
			added_at?: string
			poster_path?: string | null
		}>
	}
	recent_reviews?: Array<{
		review_id: number
		title: string
		media_type: 'movie' | 'tv'
		id?: number
		rating: number | null
		content: string | null
		created_at: string
		poster_path: string | null
	}>
}

export async function getUserProfile() {
	try {
		const { data } = await api.get('/user/profile')
		return data as { ok: true } & UserProfile | { ok: false; error: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to fetch profile'
		return { ok: false, error: String(msg) } as { ok: false; error: string }
	}
}

export type PublicUserProfile = {
	ok: true
	user: {
		user_id: number
		display_name: string
		created_at: string
	}
	stats: {
		total_reviews: number
		movie_reviews: number
		tv_reviews: number
		avg_rating: number | null
		movie_avg_rating: number | null
		tv_avg_rating: number | null
	}
	favorites: Array<{
		title: string
		media_type: 'movie' | 'tv'
		rating?: number
		id: number
		poster_path: string | null
	}>
	recent_reviews: Array<{
		review_id: number
		title: string
		media_type: 'movie' | 'tv'
		id: number
		rating: number | null
		content: string | null
		created_at: string
		poster_path: string | null
	}>
	watchlist: {
		movies: Array<{
			title: string
			media_type: 'movie'
			id?: number
			added_at?: string
			poster_path?: string | null
		}>
		tv: Array<{
			title: string
			media_type: 'tv'
			id?: number
			added_at?: string
			poster_path?: string | null
		}>
	}
}

export async function getPublicUserProfile(userId: number) {
	try {
		const { data } = await api.get(`/users/${userId}/public-profile`)
		return data as PublicUserProfile | { ok: false; error: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to fetch user profile'
		return { ok: false, error: String(msg) } as { ok: false; error: string }
	}
}

export async function addToWatchlist(userId: number, targetType: 'movie' | 'show', targetId: number) {
	try {
		const { data } = await api.post('/watchlist', {
			user_id: userId,
			target_type: targetType,
			target_id: targetId
		})
		return data as { ok: boolean; error?: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to add to watchlist'
		return { ok: false, error: String(msg) }
	}
}

export async function removeFromWatchlist(userId: number, targetType: 'movie' | 'show', targetId: number) {
	try {
		const { data } = await api.delete('/watchlist', {
			data: {
				user_id: userId,
				target_type: targetType,
				target_id: targetId
			}
		})
		return data as { ok: boolean; deleted?: number; error?: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to remove from watchlist'
		return { ok: false, error: String(msg) }
	}
}

export async function addToFavorites(userId: number, targetType: 'movie' | 'show', targetId: number) {
	try {
		const { data } = await api.post('/favorites', {
			user_id: userId,
			target_type: targetType,
			target_id: targetId
		})
		return data as { ok: boolean; error?: string; already_favorited?: boolean }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to add to favorites'
		console.error('Add to favorites error:', msg)
		return { ok: false, error: String(msg) }
	}
}

export async function removeFromFavorites(userId: number, targetType: 'movie' | 'show', targetId: number) {
	try {
		const { data } = await api.delete('/favorites', {
			data: {
				user_id: userId,
				target_type: targetType,
				target_id: targetId
			}
		})
		return data as { ok: boolean; deleted?: number; error?: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to remove from favorites'
		console.error('Remove from favorites error:', msg)
		return { ok: false, error: String(msg) }
	}
}

export async function checkFavorite(userId: number, targetType: 'movie' | 'show', targetId: number) {
	try {
		const { data } = await api.get('/favorites/check', {
			params: { user_id: userId, target_type: targetType, target_id: targetId }
		})
		return data as { ok: boolean; is_favorited: boolean; error?: string }
	} catch (err: any) {
		const errorData = err?.response?.data
		const msg = errorData?.error || err?.message || 'Failed to check favorite status'
		console.error('Check favorite error:', msg)
		return { ok: false, is_favorited: false, error: msg }
	}
}

// ============================================================================
// Analytics API Functions
// ============================================================================

export type TopRatedItem = MediaItem & {
	consolidated_rating?: number
	user_avg_rating?: number
	review_count?: number
}

export type GenreDistribution = {
	name: string
	count: number
}

export async function getTopRatedMovies(limit = 5) {
	try {
		const { data } = await api.get('/analytics/top-movies', { params: { limit } })
		return data as { results: TopRatedItem[] }
	} catch (err: any) {
		console.error('Failed to fetch top movies:', err)
		return { results: [] }
	}
}

export async function getTopRatedShows(limit = 5) {
	try {
		const { data } = await api.get('/analytics/top-shows', { params: { limit } })
		return data as { results: TopRatedItem[] }
	} catch (err: any) {
		console.error('Failed to fetch top shows:', err)
		return { results: [] }
	}
}

export async function getGenreDistribution(type: 'movie' | 'show' = 'movie') {
	try {
		const { data } = await api.get('/analytics/genre-distribution', { params: { type } })
		return data as { results: GenreDistribution[] }
	} catch (err: any) {
		console.error('Failed to fetch genre distribution:', err)
		return { results: [] }
	}
}

export type PopularWatchlistItem = {
	media_type: 'movie' | 'show'
	id: number
	title: string
	poster_path?: string | null
	watchlist_count: number
}

export type UnreviewedItem = {
	media_type: 'movie' | 'tv'
	id: number
	title: string
	poster_path?: string | null
	vote_average?: number | null
	release_date?: string | null
}

export type ActiveReviewer = {
	user_id: number
	display_name: string
	review_count: number
	avg_rating?: number | null
}

export type GenreRating = {
	name: string
	avg_rating?: number | null
	review_count: number
}

export async function getPopularWatchlists(limit = 10) {
	try {
		const { data } = await api.get('/analytics/popular-watchlists', { params: { limit } })
		return data as { results: PopularWatchlistItem[] }
	} catch (err: any) {
		console.error('Failed to fetch popular watchlists:', err)
		return { results: [] }
	}
}

export async function getUnreviewedTitles(type: 'all' | 'movie' | 'show' = 'all', limit = 20) {
	try {
		const { data } = await api.get('/analytics/unreviewed', { params: { type, limit } })
		return data as { results: UnreviewedItem[] }
	} catch (err: any) {
		console.error('Failed to fetch unreviewed titles:', err)
		return { results: [] }
	}
}

export async function getActiveReviewers(minReviews = 3, limit = 10) {
	try {
		const { data } = await api.get('/analytics/active-reviewers', { params: { min_reviews: minReviews, limit } })
		return data as { results: ActiveReviewer[] }
	} catch (err: any) {
		console.error('Failed to fetch active reviewers:', err)
		return { results: [] }
	}
}

export async function getGenreRatings(type: 'movie' | 'show' = 'movie') {
	try {
		const { data } = await api.get('/analytics/genre-ratings', { params: { type } })
		return data as { results: GenreRating[] }
	} catch (err: any) {
		console.error('Failed to fetch genre ratings:', err)
		return { results: [] }
	}
}
