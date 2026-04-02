(function () {
    const modal = document.getElementById("detailsModal");
    const synopsisEl = document.getElementById("modalSynopsis");
    const coverEl = document.getElementById("modalCover");
    const coverBackdropEl = document.getElementById("modalCoverBackdrop");
    const titleDetailEl = document.getElementById("modalTitleDetail");
    const gameIdDetailEl = document.getElementById("modalGameIdDetail");
    const releaseWrapEl = document.getElementById("modalReleaseWrap");
    const releaseEl = document.getElementById("modalRelease");
    const ratingImgEl = document.getElementById("modalRatingImg");
    const genreTagEl = document.getElementById("modalGenreTag");
    const publisherTagEl = document.getElementById("modalPublisherTag");
    const developerTagEl = document.getElementById("modalDeveloperTag");

    if (!modal) {
        return;
    }

    let currentGameId = "";
    let currentSource = "recommendations";

    async function loadAverages() {
        if (!currentGameId) return;

        if (currentSource === "time_played") {
            loadPlaytimeStats();
        } else {
            loadRecommendationStats();
        }
    }

    async function loadRecommendationStats() {
        if (!currentGameId) return;
        const gender = document.getElementById("avgGender").value;
        const ageMin = document.getElementById("avgAgeMin").value;
        const ageMax = document.getElementById("avgAgeMax").value;
        const params = new URLSearchParams({ game_id: currentGameId });
        if (gender) params.append("gender", gender);
        if (ageMin) params.append("age_min", ageMin);
        if (ageMax) params.append("age_max", ageMax);

        const summary = document.getElementById("modalAvgSummary");
        summary.textContent = "Loading...";
        try {
            const response = await fetch(`/recommendations/averages?${params.toString()}`);
            const data = await response.json();
            if (!data || !data.total) {
                summary.textContent = "No community data for this game.";
                document.getElementById("avgScoreText").textContent = "-";
                document.getElementById("avgAppealText").textContent = "-";
                document.getElementById("avgMoodText").textContent = "-";
                document.getElementById("avgFriendText").textContent = "-";
                document.getElementById("avgAppealLabel").textContent = "-";
                document.getElementById("avgMoodLabel").textContent = "-";
                document.getElementById("avgFriendLabel").textContent = "-";
                document.getElementById("avgScoreBar").style.width = "0%";
                document.getElementById("avgAppealBar").style.width = "0%";
                document.getElementById("avgMoodBar").style.width = "0%";
                document.getElementById("avgFriendBar").style.width = "0%";
                return;
            }

            const avgScore = Math.round(Number(data.avg_score || 0));
            const appealPct = Math.round(Number(data.avg_appeal || 0) * 100);
            const moodPct = Math.round(Number(data.avg_mood || 0) * 100);
            const friendPct = Math.round(Number(data.avg_friend || 0) * 100);

            summary.textContent = `${data.total} ratings for this game`;
            document.getElementById("avgScoreText").textContent = `${avgScore}%`;
            document.getElementById("avgAppealText").textContent = `${appealPct}%`;
            document.getElementById("avgMoodText").textContent = `${moodPct}%`;
            document.getElementById("avgFriendText").textContent = `${friendPct}%`;
            const avgScoreBar = document.getElementById("avgScoreBar");
            const avgScoreText = document.getElementById("avgScoreText");
            avgScoreBar.style.width = `${avgScore}%`;
            if (avgScore >= 80) {
                avgScoreBar.className = "h-full rounded-full flex items-center justify-end bg-emerald-400";
                avgScoreText.className = "pr-2 text-xs uppercase tracking-wider text-emerald-900";
            } else if (avgScore >= 50) {
                avgScoreBar.className = "h-full rounded-full flex items-center justify-end bg-yellow-400";
                avgScoreText.className = "pr-2 text-xs uppercase tracking-wider text-yellow-900";
            } else {
                avgScoreBar.className = "h-full rounded-full flex items-center justify-end bg-red-400";
                avgScoreText.className = "pr-2 text-xs uppercase tracking-wider text-red-900";
            }
            document.getElementById("avgAppealBar").style.width = `${appealPct}%`;
            document.getElementById("avgMoodBar").style.width = `${moodPct}%`;
            document.getElementById("avgFriendBar").style.width = `${friendPct}%`;

            const appealIcon = document.getElementById("avgAppealIcon");
            const appealLabel = document.getElementById("avgAppealLabel");
            if (appealPct >= 50) {
                appealIcon.src = "/static/icon/sword.svg";
                appealLabel.textContent = "Gamers";
            } else {
                appealIcon.src = "/static/icon/earth.svg";
                appealLabel.textContent = "Everyone";
            }

            const moodIcon = document.getElementById("avgMoodIcon");
            const moodLabel = document.getElementById("avgMoodLabel");
            if (moodPct >= 50) {
                moodIcon.src = "/static/icon/gamepad.svg";
                moodLabel.textContent = "Hardcore";
            } else {
                moodIcon.src = "/static/icon/house.svg";
                moodLabel.textContent = "Casual";
            }

            const friendIcon = document.getElementById("avgFriendIcon");
            const friendLabel = document.getElementById("avgFriendLabel");
            if (friendPct >= 50) {
                friendIcon.src = "/static/icon/users.svg";
                friendLabel.textContent = "With friends";
            } else {
                friendIcon.src = "/static/icon/user.svg";
                friendLabel.textContent = "Solo";
            }
            document.getElementById("recommendationStatsSection").style.display = "block";
            document.getElementById("timePlayedStatsSection").style.display = "none";
        } catch (err) {
            summary.textContent = "Unable to load community data.";
        }
    }

    async function loadPlaytimeStats() {
        if (!currentGameId) return;
        try {
            const response = await fetch(`/time_played/stats?game_id=${currentGameId}`);
            const data = await response.json();
            if (!data || !data.total_players) {
                document.getElementById("totalPlayersText").textContent = "-";
                document.getElementById("totalTimeText").textContent = "-";
                document.getElementById("avgTimePerPlayerText").textContent = "-";
                return;
            }

            function formatMinutes(minutes) {
                minutes = parseInt(minutes) || 0;
                const days = Math.floor(minutes / (24 * 60));
                const hours = Math.floor((minutes % (24 * 60)) / 60);
                const mins = minutes % 60;
                const parts = [];
                if (days > 0) parts.push(`${days}d`);
                if (hours > 0) parts.push(`${hours}h`);
                if (mins > 0 || parts.length === 0) parts.push(`${mins}m`);
                return parts.join(" ");
            }

            document.getElementById("totalPlayersText").textContent = data.total_players;
            document.getElementById("totalTimeText").textContent = formatMinutes(data.total_minutes);
            document.getElementById("avgTimePerPlayerText").textContent = formatMinutes(data.avg_minutes_per_player);
            document.getElementById("recommendationStatsSection").style.display = "none";
            document.getElementById("timePlayedStatsSection").style.display = "block";
        } catch (err) {
            document.getElementById("totalPlayersText").textContent = "-";
            document.getElementById("totalTimeText").textContent = "-";
            document.getElementById("avgTimePerPlayerText").textContent = "-";
        }
    }

    function formatGenres(raw) {
        if (!raw) return "-";
        return raw.split(",").map((g) => g.trim().toUpperCase()).filter(Boolean).join(" | ") || "-";
    }

    function openModal(data) {
        synopsisEl.textContent = data.synopsis || "No synopsis available.";
        currentGameId = data.gameId || "";
        currentSource = data.source || "recommendations";

        coverEl.dataset.tried = "";
        coverEl.dataset.fallback = data.coverFallback || "";
        coverEl.src = data.coverUrl || "";
        coverEl.style.display = data.coverUrl ? "block" : "none";
        coverBackdropEl.style.backgroundImage = data.coverUrl ? `url('${data.coverUrl}')` : "";
        titleDetailEl.textContent = data.title || "";
        gameIdDetailEl.textContent = data.gameId || "-";
        if (data.releaseYear) {
            releaseEl.textContent = data.releaseYear;
            releaseWrapEl.classList.remove("hidden");
        } else {
            releaseWrapEl.classList.add("hidden");
        }

        const hasFullRating = Boolean(data.ratingType && data.ratingValue);
        if (hasFullRating) {
            const ratingText = `${data.ratingType}-${data.ratingValue}`;
            ratingImgEl.src = `/static/ratings/${ratingText}.jpg`;
            ratingImgEl.classList.remove("hidden");
        } else {
            ratingImgEl.src = "";
            ratingImgEl.classList.add("hidden");
        }

        const genreText = formatGenres(data.genre);
        if (genreText !== "-") {
            genreTagEl.textContent = genreText;
            genreTagEl.classList.remove("hidden");
        } else {
            genreTagEl.classList.add("hidden");
        }

        if (data.publisher) {
            publisherTagEl.textContent = data.publisher;
            publisherTagEl.classList.remove("hidden");
        } else {
            publisherTagEl.classList.add("hidden");
        }

        if (data.developer) {
            developerTagEl.textContent = data.developer;
            developerTagEl.classList.remove("hidden");
        } else {
            developerTagEl.classList.add("hidden");
        }

        modal.classList.remove("hidden");
        loadAverages();
    }

    function closeModal() {
        modal.classList.add("hidden");
    }

    document.querySelectorAll("[data-modal-open]").forEach((button) => {
        button.addEventListener("click", () => {
            openModal({
                title: button.dataset.title,
                gameId: button.dataset.gameId,
                releaseYear: button.dataset.releaseYear,
                genre: button.dataset.genre,
                developer: button.dataset.developer,
                publisher: button.dataset.publisher,
                ratingType: button.dataset.ratingType,
                ratingValue: button.dataset.ratingValue,
                synopsis: button.dataset.synopsis,
                coverUrl: button.dataset.coverUrl,
                coverFallback: button.dataset.coverFallback,
                source: button.dataset.source || "recommendations"
            });
        });
    });

    document.querySelectorAll("[data-modal-close]").forEach((el) => {
        el.addEventListener("click", closeModal);
    });

    document.getElementById("avgGender").addEventListener("change", loadAverages);
    document.getElementById("avgAgeMin").addEventListener("input", loadAverages);
    document.getElementById("avgAgeMax").addEventListener("input", loadAverages);
})();
